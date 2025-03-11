#[cfg(any(feature = "rustls", feature = "native-tls",))]
use super::tls_stream::TlsStream;
use crate::mssql::connection::tds::{
    codec::{Decode, Encode, PacketHeader, PacketStatus, PacketType},
    HEADER_BYTES,
};
use bytes::BytesMut;
use std::io::{Read, Write};
use std::{cmp, io};
use tracing::{event, Level};

/// A wrapper to handle either TLS or bare connections.
pub(crate) enum MaybeTlsStream<S: Read + Write + Send> {
    Raw(S),
    #[cfg(any(feature = "rustls", feature = "native-tls",))]
    Tls(TlsStream<TlsPreloginWrapper<S>>),
}

#[cfg(any(feature = "rustls", feature = "native-tls",))]
impl<S: Read + Write + Send> MaybeTlsStream<S> {
    pub(crate) fn into_inner(self) -> S {
        match self {
            Self::Raw(s) => s,
            #[cfg(any(feature = "rustls", feature = "native-tls",))]
            Self::Tls(mut tls) => tls.get_mut().stream.take().unwrap(),
        }
    }
}

impl<S: Read + Write + Send> Read for MaybeTlsStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            MaybeTlsStream::Raw(s) => s.read(buf),
            #[cfg(any(feature = "rustls", feature = "native-tls",))]
            MaybeTlsStream::Tls(s) => s.read(buf),
        }
    }
}

impl<S: Read + Write + Send> Write for MaybeTlsStream<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            MaybeTlsStream::Raw(s) => s.write(buf),
            #[cfg(any(feature = "rustls", feature = "native-tls",))]
            MaybeTlsStream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            MaybeTlsStream::Raw(s) => s.flush(),
            #[cfg(any(feature = "rustls", feature = "native-tls",))]
            MaybeTlsStream::Tls(s) => s.flush(),
        }
    }
}

/// On TLS handshake, the server expects to get and sends back normal TDS
/// packets. To use a common TLS library, we must implement a wrapper for
/// packet handling on this stage.
///
/// What it does is it interferes on handshake for TDS packet handling,
/// and when complete, just passes the calls to the underlying connection.
pub(crate) struct TlsPreloginWrapper<S> {
    stream: Option<S>,
    pending_handshake: bool,

    header_buf: [u8; HEADER_BYTES],
    header_pos: usize,
    read_remaining: usize,

    wr_buf: Vec<u8>,
    header_written: bool,
}

#[cfg(any(feature = "rustls", feature = "native-tls",))]
impl<S> TlsPreloginWrapper<S> {
    pub(crate) fn new(stream: S) -> Self {
        TlsPreloginWrapper {
            stream: Some(stream),
            pending_handshake: true,

            header_buf: [0u8; HEADER_BYTES],
            header_pos: 0,
            read_remaining: 0,
            wr_buf: vec![0u8; HEADER_BYTES],
            header_written: false,
        }
    }

    pub(crate) fn handshake_complete(&mut self) {
        self.pending_handshake = false;
    }
}

impl<S: Read + Write + Send> Read for TlsPreloginWrapper<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Normal operation does not need any extra treatment, we handle packets
        // in the codec.
        if !self.pending_handshake {
            return self.stream.as_mut().unwrap().read(buf);
        }

        // Read the headers separately and do not send them to the Tls
        // connection handling.
        if !self.header_buf[self.header_pos..].is_empty() {
            while !self.header_buf[self.header_pos..].is_empty() {
                let read = self
                    .stream
                    .as_mut()
                    .unwrap()
                    .read(&mut self.header_buf[self.header_pos..])?;

                if read == 0 {
                    return Ok(0);
                }

                self.header_pos += read;
            }

            let header = PacketHeader::decode(&mut BytesMut::from(&self.header_buf[..]))
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

            // We only get pre-login packets in the handshake process.
            assert_eq!(header.r#type(), PacketType::PreLogin);

            // And we know from this point on how much data we should expect
            self.read_remaining = header.length() as usize - HEADER_BYTES;

            event!(
                Level::TRACE,
                "Reading packet of {} bytes",
                self.read_remaining,
            );
        }

        let max_read = cmp::min(self.read_remaining, buf.len());

        // TLS connector gets whatever we have after the header.
        let read = self.stream.as_mut().unwrap().read(&mut buf[..max_read])?;

        self.read_remaining -= read;

        // All data is read, after this we're expecting a new header.
        if self.read_remaining == 0 {
            self.header_pos = 0;
        }

        Ok(read)
    }
}

impl<S: Read + Write + Send> Write for TlsPreloginWrapper<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Normal operation does not need any extra treatment, we handle
        // packets in the codec.
        if !self.pending_handshake {
            return self.stream.as_mut().unwrap().write(buf);
        }

        // Buffering data.
        self.wr_buf.extend_from_slice(buf);

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // If on handshake mode, wraps the data to a TDS packet before sending.
        if self.pending_handshake && self.wr_buf.len() > HEADER_BYTES {
            if !self.header_written {
                let mut header = PacketHeader::new(self.wr_buf.len(), 0);

                header.set_type(PacketType::PreLogin);
                header.set_status(PacketStatus::EndOfMessage);

                header
                    .encode(&mut &mut self.wr_buf[0..HEADER_BYTES])
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "Could not encode header.")
                    })?;

                self.header_written = true;
            }

            while !self.wr_buf.is_empty() {
                event!(
                    Level::TRACE,
                    "Writing a packet of {} bytes",
                    self.wr_buf.len(),
                );

                let written = self.stream.as_mut().unwrap().write(&self.wr_buf)?;

                self.wr_buf.drain(..written);
            }

            self.wr_buf.resize(HEADER_BYTES, 0);
            self.header_written = false;
        }

        self.stream.as_mut().unwrap().flush()
    }
}
