use crate::mssql::connection::tds::codec::TokenSspi;
use crate::mssql::connection::{
    client::Connection,
    tds::codec::{
        TokenColMetaData, TokenDone, TokenEnvChange, TokenError, TokenFeatureExtAck, TokenInfo,
        TokenLoginAck, TokenOrder, TokenReturnValue, TokenRow,
    },
    Error, SqlReadBytes, TokenType,
};
use std::io::{Read, Write};
use std::{convert::TryFrom, sync::Arc};
use tracing::{event, Level};

use super::BoxIter;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum ReceivedToken {
    NewResultset(Arc<TokenColMetaData<'static>>),
    Row(TokenRow<'static>),
    Done(TokenDone),
    DoneInProc(TokenDone),
    DoneProc(TokenDone),
    ReturnStatus(u32),
    ReturnValue(TokenReturnValue),
    Order(TokenOrder),
    EnvChange(TokenEnvChange),
    Info(TokenInfo),
    LoginAck(TokenLoginAck),
    Sspi(TokenSspi),
    FeatureExtAck(TokenFeatureExtAck),
    Error(TokenError),
}

pub(crate) struct TokenStream<'a, S: Read + Write + Send> {
    conn: &'a mut Connection<S>,
    last_error: Option<Error>,
}

impl<'a, S> TokenStream<'a, S>
where
    S: Read + Write + Send,
{
    pub(crate) fn new(conn: &'a mut Connection<S>) -> Self {
        Self {
            conn,
            last_error: None,
        }
    }

    pub(crate) fn flush_done(self) -> crate::mssql::connection::Result<TokenDone> {
        let mut stream = self.try_unfold();
        let mut last_error = None;
        let mut routing = None;

        loop {
            match stream.next().transpose()? {
                Some(ReceivedToken::Error(error)) => {
                    if last_error.is_none() {
                        last_error = Some(error);
                    }
                }
                Some(ReceivedToken::Done(token)) => match (last_error, routing) {
                    (Some(error), _) => return Err(Error::Server(error)),
                    (_, Some(routing)) => return Err(routing),
                    (_, _) => return Ok(token),
                },
                Some(ReceivedToken::EnvChange(TokenEnvChange::Routing { host, port })) => {
                    routing = Some(Error::Routing { host, port });
                }
                Some(_) => (),
                None => {
                    return Err(crate::mssql::connection::Error::Protocol(
                        "Never got DONE token.".into(),
                    ))
                }
            }
        }
    }

    #[cfg(windows)]
    pub(crate) fn flush_sspi(self) -> crate::mssql::connection::Result<TokenSspi> {
        let mut stream = self.try_unfold();
        let mut last_error = None;

        loop {
            match stream.next().transpose()? {
                Some(ReceivedToken::Error(error)) => {
                    if last_error.is_none() {
                        last_error = Some(error);
                    }
                }
                Some(ReceivedToken::Sspi(token)) => return Ok(token),
                Some(_) => (),
                None => match last_error {
                    Some(err) => return Err(crate::mssql::connection::Error::Server(err)),
                    None => {
                        return Err(crate::mssql::connection::Error::Protocol(
                            "Never got SSPI token.".into(),
                        ))
                    }
                },
            }
        }
    }

    fn get_col_metadata(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let meta = Arc::new(TokenColMetaData::decode(self.conn)?);
        self.conn.context_mut().set_last_meta(meta.clone());

        event!(Level::TRACE, ?meta);

        Ok(ReceivedToken::NewResultset(meta))
    }

    fn get_row(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let return_value = TokenRow::decode(self.conn)?;

        event!(Level::TRACE, message = ?return_value);
        Ok(ReceivedToken::Row(return_value))
    }

    fn get_nbc_row(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let return_value = TokenRow::decode_nbc(self.conn)?;

        event!(Level::TRACE, message = ?return_value);
        Ok(ReceivedToken::Row(return_value))
    }

    fn get_return_value(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let return_value = TokenReturnValue::decode(self.conn)?;
        event!(Level::TRACE, message = ?return_value);
        Ok(ReceivedToken::ReturnValue(return_value))
    }

    fn get_return_status(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let status = self.conn.read_u32_le()?;
        Ok(ReceivedToken::ReturnStatus(status))
    }

    fn get_error(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let err = TokenError::decode(self.conn)?;
        if self.last_error.is_none() {
            self.last_error = Some(Error::Server(err.clone()));
        }

        event!(Level::ERROR, message = %err.message, code = err.code);
        Ok(ReceivedToken::Error(err))
    }

    fn get_order(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let order = TokenOrder::decode(self.conn)?;
        event!(Level::TRACE, message = ?order);
        Ok(ReceivedToken::Order(order))
    }

    fn get_done_value(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let done = TokenDone::decode(self.conn)?;
        event!(Level::TRACE, "{}", done);
        Ok(ReceivedToken::Done(done))
    }

    fn get_done_proc_value(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let done = TokenDone::decode(self.conn)?;
        event!(Level::TRACE, "{}", done);
        Ok(ReceivedToken::DoneProc(done))
    }

    fn get_done_in_proc_value(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let done = TokenDone::decode(self.conn)?;
        event!(Level::TRACE, "{}", done);
        Ok(ReceivedToken::DoneInProc(done))
    }

    fn get_env_change(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let change = TokenEnvChange::decode(self.conn)?;

        match change {
            TokenEnvChange::PacketSize(new_size, _) => {
                self.conn.context_mut().set_packet_size(new_size);
            }
            TokenEnvChange::BeginTransaction(desc) => {
                self.conn.context_mut().set_transaction_descriptor(desc);
            }
            TokenEnvChange::CommitTransaction
            | TokenEnvChange::RollbackTransaction
            | TokenEnvChange::DefectTransaction => {
                self.conn.context_mut().set_transaction_descriptor([0; 8]);
            }
            _ => (),
        }

        event!(Level::INFO, "{}", change);

        Ok(ReceivedToken::EnvChange(change))
    }

    fn get_info(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let info = TokenInfo::decode(self.conn)?;
        event!(Level::INFO, "{}", info.message);
        Ok(ReceivedToken::Info(info))
    }

    fn get_login_ack(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let ack = TokenLoginAck::decode(self.conn)?;
        event!(Level::INFO, "{} version {}", ack.prog_name, ack.version);
        Ok(ReceivedToken::LoginAck(ack))
    }

    fn get_feature_ext_ack(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let ack = TokenFeatureExtAck::decode(self.conn)?;
        event!(
            Level::INFO,
            "FeatureExtAck with {} features",
            ack.features.len()
        );
        Ok(ReceivedToken::FeatureExtAck(ack))
    }

    fn get_sspi(&mut self) -> crate::mssql::connection::Result<ReceivedToken> {
        let sspi = TokenSspi::decode_async(self.conn)?;
        event!(Level::TRACE, "SSPI response");
        Ok(ReceivedToken::Sspi(sspi))
    }

    pub(crate) fn try_unfold(
        mut self,
    ) -> BoxIter<'a, crate::mssql::connection::Result<ReceivedToken>> {
        let stream = std::iter::from_fn(move || {
            if self.conn.is_eof() {
                match &self.last_error {
                    None => return None,
                    Some(error) if error.code() == Some(266) => return None,
                    Some(error) => return Some(Err(error.to_owned())),
                }
            }

            let mut res = || {
                let ty_byte = self.conn.read_u8()?;

                let ty = TokenType::try_from(ty_byte).map_err(|_| {
                    Error::Protocol(format!("invalid token type {:x}", ty_byte).into())
                })?;

                let token = match ty {
                    TokenType::ReturnStatus => self.get_return_status()?,
                    TokenType::ColMetaData => self.get_col_metadata()?,
                    TokenType::Row => self.get_row()?,
                    TokenType::NbcRow => self.get_nbc_row()?,
                    TokenType::Done => self.get_done_value()?,
                    TokenType::DoneProc => self.get_done_proc_value()?,
                    TokenType::DoneInProc => self.get_done_in_proc_value()?,
                    TokenType::ReturnValue => self.get_return_value()?,
                    TokenType::Error => self.get_error()?,
                    TokenType::Order => self.get_order()?,
                    TokenType::EnvChange => self.get_env_change()?,
                    TokenType::Info => self.get_info()?,
                    TokenType::LoginAck => self.get_login_ack()?,
                    TokenType::Sspi => self.get_sspi()?,
                    TokenType::FeatureExtAck => self.get_feature_ext_ack()?,
                    _ => panic!("Token {:?} unimplemented!", ty),
                };
                Ok(token)
            };

            Some(res())
        });

        Box::new(stream)
    }
}
