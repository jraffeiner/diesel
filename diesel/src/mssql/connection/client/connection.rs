#[cfg(any(feature = "rustls", feature = "native-tls",))]
use crate::mssql::connection::client::{tls::TlsPreloginWrapper, tls_stream::create_tls_stream};
use crate::mssql::connection::{
    client::{tls::MaybeTlsStream, AuthMethod, Config},
    framed::Framed,
    sink::Sink,
    tds::{
        codec::{
            self, Encode, LoginMessage, Packet, PacketCodec, PacketHeader, PacketStatus,
            PreloginMessage, TokenDone,
        },
        stream::TokenStream,
        Context, HEADER_BYTES,
    },
    EncryptionLevel, SqlReadBytes,
};
use bytes::BytesMut;
#[cfg(windows)]
use codec::TokenSspi;
use pretty_hex::*;
use std::io::{Read, Write};
use std::{cmp, fmt::Debug, io};
use tracing::{event, Level};
#[cfg(all(windows, feature = "winauth"))]
use winauth::{windows::NtlmSspiBuilder, NextBytes};

/// A `Connection` is an abstraction between the [`Client`] and the server. It
/// can be used as a `Stream` to fetch [`Packet`]s from and to `send` packets
/// splitting them to the negotiated limit automatically.
///
/// `Connection` is not meant to use directly, but as an abstraction layer for
/// the numerous `Stream`s for easy packet handling.
///
/// [`Client`]: struct.Encode.html
/// [`Packet`]: ../protocol/codec/struct.Packet.html
pub(crate) struct Connection<S>
where
    S: Read + Write + Send,
{
    transport: Framed<MaybeTlsStream<S>, PacketCodec>,
    flushed: bool,
    context: Context,
    buf: BytesMut,
}

impl<S: Read + Write + Send> Debug for Connection<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("transport", &"Framed<..>")
            .field("flushed", &self.flushed)
            .field("context", &self.context)
            .field("buf", &self.buf.as_ref().hex_dump())
            .finish()
    }
}

impl<S: Read + Write + Send> Connection<S> {
    /// Creates a new connection
    pub(crate) fn connect(
        config: Config,
        tcp_stream: S,
    ) -> crate::mssql::connection::Result<Connection<S>> {
        let context = {
            let mut context = Context::new();
            context.set_spn(config.get_host(), config.get_port());
            context
        };

        let transport = Framed::new(MaybeTlsStream::Raw(tcp_stream), PacketCodec);

        let mut connection = Self {
            transport,
            context,
            flushed: false,
            buf: BytesMut::new(),
        };

        let fed_auth_required = matches!(config.auth, AuthMethod::AADToken(_));

        let prelogin = connection.prelogin(config.encryption, fed_auth_required)?;

        let encryption = prelogin.negotiated_encryption(config.encryption);

        let connection = connection.tls_handshake(&config, encryption)?;

        let mut connection = connection.login(
            config.auth,
            encryption,
            config.database,
            config.host,
            config.application_name,
            config.readonly,
            prelogin,
        )?;

        connection.flush_done()?;

        Ok(connection)
    }

    /// Flush the incoming token stream until receiving `DONE` token.
    fn flush_done(&mut self) -> crate::mssql::connection::Result<TokenDone> {
        TokenStream::new(self).flush_done()
    }

    #[cfg(windows)]
    /// Flush the incoming token stream until receiving `SSPI` token.
    fn flush_sspi(&mut self) -> crate::mssql::connection::Result<TokenSspi> {
        TokenStream::new(self).flush_sspi()
    }

    #[cfg(any(feature = "rustls", feature = "native-tls",))]
    fn post_login_encryption(mut self, encryption: EncryptionLevel) -> Self {
        if let EncryptionLevel::Off = encryption {
            event!(
                Level::WARN,
                "Turning TLS off after a login. All traffic from here on is not encrypted.",
            );

            let Self { transport, .. } = self;
            let tcp = transport.into_inner().into_inner();
            self.transport = Framed::new(MaybeTlsStream::Raw(tcp), PacketCodec);
        }

        self
    }

    #[cfg(not(any(feature = "rustls", feature = "native-tls",)))]
    fn post_login_encryption(self, _: EncryptionLevel) -> Self {
        self
    }

    /// Send an item to the wire. Header should define the item type and item should implement
    /// [`Encode`], defining the byte structure for the wire.
    ///
    /// The `send` will split the packet into multiple packets if bigger than
    /// the negotiated packet size, and handle flushing to the wire in an optimal way.
    ///
    /// [`Encode`]: ../protocol/codec/trait.Encode.html
    pub(crate) fn send<E>(
        &mut self,
        mut header: PacketHeader,
        item: E,
    ) -> crate::mssql::connection::Result<()>
    where
        E: Sized + Encode<BytesMut>,
    {
        self.flushed = false;
        let packet_size = (self.context.packet_size() as usize) - HEADER_BYTES;

        let mut payload = BytesMut::new();
        item.encode(&mut payload)?;

        while !payload.is_empty() {
            let writable = cmp::min(payload.len(), packet_size);
            let split_payload = payload.split_to(writable);

            if payload.is_empty() {
                header.set_status(PacketStatus::EndOfMessage);
            } else {
                header.set_status(PacketStatus::NormalMessage);
            }

            event!(
                Level::TRACE,
                "Sending a packet ({} bytes)",
                split_payload.len() + HEADER_BYTES,
            );

            self.write_to_wire(header, split_payload)?;
        }

        self.flush_sink()?;

        Ok(())
    }

    /// Sends a packet of data to the database.
    ///
    /// # Warning
    ///
    /// Please be sure the packet size doesn't exceed the largest allowed size
    /// dictaded by the server.
    pub(crate) fn write_to_wire(
        &mut self,
        header: PacketHeader,
        data: BytesMut,
    ) -> crate::mssql::connection::Result<()> {
        self.flushed = false;

        let packet = Packet::new(header, data);
        self.transport.send(packet)?;

        Ok(())
    }

    /// Sends all pending packages to the wire.
    pub(crate) fn flush_sink(&mut self) -> crate::mssql::connection::Result<()> {
        self.transport.flush()
    }

    /// Cleans the packet stream from previous use. It is important to use the
    /// whole stream before using the connection again. Flushing the stream
    /// makes sure we don't have any old data causing undefined behaviour after
    /// previous queries.
    ///
    /// Calling this will slow down the queries if stream is still dirty if all
    /// results are not handled.
    pub(crate) fn flush_stream(&mut self) -> crate::mssql::connection::Result<()> {
        self.buf.truncate(0);

        if self.flushed {
            return Ok(());
        }

        while let Some(packet) = self.next().transpose()? {
            event!(
                Level::WARN,
                "Flushing unhandled packet from the wire. Please consume your streams!",
            );

            let is_last = packet.is_last();

            if is_last {
                break;
            }
        }

        Ok(())
    }

    /// True if the underlying stream has no more data and is consumed
    /// completely.
    pub(crate) fn is_eof(&self) -> bool {
        self.flushed && self.buf.is_empty()
    }

    /// A message sent by the client to set up context for login. The server
    /// responds to a client PRELOGIN message with a message of packet header
    /// type 0x04 and with the packet data containing a PRELOGIN structure.
    ///
    /// This message stream is also used to wrap the TLS handshake payload if
    /// encryption is needed. In this scenario, where PRELOGIN message is
    /// transporting the TLS handshake payload, the packet data is simply the
    /// raw bytes of the TLS handshake payload.
    fn prelogin(
        &mut self,
        encryption: EncryptionLevel,
        fed_auth_required: bool,
    ) -> crate::mssql::connection::Result<PreloginMessage> {
        let mut msg = PreloginMessage::new();
        msg.encryption = encryption;
        msg.fed_auth_required = fed_auth_required;

        let id = self.context.next_packet_id();
        self.send(PacketHeader::pre_login(id), msg)?;

        let response: PreloginMessage = codec::collect_from(self)?;
        // threadid (should be empty when sent from server to client)
        debug_assert_eq!(response.thread_id, 0);
        Ok(response)
    }

    /// Defines the login record rules with SQL Server. Authentication with
    /// connection options.
    #[allow(clippy::too_many_arguments)]
    fn login(
        mut self,
        auth: AuthMethod,
        encryption: EncryptionLevel,
        db: Option<String>,
        server_name: Option<String>,
        application_name: Option<String>,
        readonly: bool,
        prelogin: PreloginMessage,
    ) -> crate::mssql::connection::Result<Self> {
        let mut login_message = LoginMessage::new();

        if let Some(db) = db {
            login_message.db_name(db);
        }

        if let Some(server_name) = server_name {
            login_message.server_name(server_name);
        }

        if let Some(app_name) = application_name {
            login_message.app_name(app_name);
        }

        login_message.readonly(readonly);

        match auth {
            #[cfg(all(windows, feature = "winauth"))]
            AuthMethod::Integrated => {
                let mut client = NtlmSspiBuilder::new()
                    .target_spn(self.context.spn())
                    .build()?;

                login_message.integrated_security(client.next_bytes(None)?);

                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message)?;

                self = self.post_login_encryption(encryption);

                let sspi_bytes = self.flush_sspi()?;

                match client.next_bytes(Some(sspi_bytes.as_ref()))? {
                    Some(sspi_response) => {
                        event!(Level::TRACE, sspi_response_len = sspi_response.len());

                        let id = self.context.next_packet_id();
                        let header = PacketHeader::login(id);

                        let token = TokenSspi::new(sspi_response);
                        self.send(header, token)?;
                    }
                    None => unreachable!(),
                }
            }
            #[cfg(all(windows, feature = "winauth"))]
            AuthMethod::Windows(auth) => {
                let spn = self.context.spn().to_string();
                let builder = winauth::NtlmV2ClientBuilder::new().target_spn(spn);
                let mut client = builder.build(auth.domain, auth.user, auth.password);

                login_message.integrated_security(client.next_bytes(None)?);

                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message)?;

                self = self.post_login_encryption(encryption);

                let sspi_bytes = self.flush_sspi()?;

                match client.next_bytes(Some(sspi_bytes.as_ref()))? {
                    Some(sspi_response) => {
                        event!(Level::TRACE, sspi_response_len = sspi_response.len());

                        let id = self.context.next_packet_id();
                        let header = PacketHeader::login(id);

                        let token = TokenSspi::new(sspi_response);
                        self.send(header, token)?;
                    }
                    None => unreachable!(),
                }
            }
            AuthMethod::None => {
                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message)?;
                self = self.post_login_encryption(encryption);
            }
            AuthMethod::SqlServer(auth) => {
                login_message.user_name(auth.user());
                login_message.password(auth.password());

                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message)?;
                self = self.post_login_encryption(encryption);
            }
            AuthMethod::AADToken(token) => {
                login_message.aad_token(token, prelogin.fed_auth_required, prelogin.nonce);
                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message)?;
                self = self.post_login_encryption(encryption);
            }
        }

        Ok(self)
    }

    /// Implements the TLS handshake with the SQL Server.
    #[cfg(any(feature = "rustls", feature = "native-tls",))]
    fn tls_handshake(
        self,
        config: &Config,
        encryption: EncryptionLevel,
    ) -> crate::mssql::connection::Result<Self> {
        if encryption != EncryptionLevel::NotSupported {
            event!(Level::INFO, "Performing a TLS handshake");

            let Self {
                transport, context, ..
            } = self;
            let mut stream = match transport.into_inner() {
                MaybeTlsStream::Raw(tcp) => {
                    create_tls_stream(config, TlsPreloginWrapper::new(tcp))?
                }
                _ => unreachable!(),
            };

            stream.get_mut().handshake_complete();
            event!(Level::INFO, "TLS handshake successful");

            let transport = Framed::new(MaybeTlsStream::Tls(stream), PacketCodec);

            Ok(Self {
                transport,
                context,
                flushed: false,
                buf: BytesMut::new(),
            })
        } else {
            event!(
                Level::WARN,
                "TLS encryption is not enabled. All traffic including the login credentials are not encrypted."
            );

            Ok(self)
        }
    }

    /// Implements the TLS handshake with the SQL Server.
    #[cfg(not(any(feature = "rustls", feature = "native-tls",)))]
    fn tls_handshake(
        self,
        _: &Config,
        _: EncryptionLevel,
    ) -> crate::mssql::connection::Result<Self> {
        event!(
            Level::WARN,
            "TLS encryption is not enabled. All traffic including the login credentials are not encrypted."
        );

        Ok(self)
    }
}

impl<S: Read + Write + Send> Iterator for Connection<S> {
    type Item = crate::mssql::connection::Result<Packet>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.transport.next() {
            Some(Ok(packet)) => {
                self.flushed = packet.is_last();
                Some(Ok(packet))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

impl<S: Read + Write + Send> io::Read for Connection<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let size = buf.len();

        if self.buf.len() < size {
            while let Some(item) = self.next() {
                match item {
                    Ok(packet) => {
                        let (_, payload) = packet.into_parts();
                        self.buf.extend(payload);

                        if self.buf.len() >= size {
                            break;
                        }
                    }
                    Err(e) => return Err(io::Error::new(io::ErrorKind::BrokenPipe, e.to_string())),
                }
            }

            // Got EOF before having all the data.
            if self.buf.len() < size {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "No more packets in the wire",
                ));
            }
        }

        buf.copy_from_slice(self.buf.split_to(size).as_ref());
        Ok(size)
    }
}

impl<S: Read + Write + Send> SqlReadBytes for Connection<S> {
    /// Hex dump of the current buffer.
    fn debug_buffer(&self) {
        dbg!(self.buf.as_ref().hex_dump());
    }

    /// The current execution context.
    fn context(&self) -> &Context {
        &self.context
    }

    /// A mutable reference to the current execution context.
    fn context_mut(&mut self) -> &mut Context {
        &mut self.context
    }
}
