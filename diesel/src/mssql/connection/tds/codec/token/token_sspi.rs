use crate::mssql::connection::{sql_read_bytes::SqlReadBytes, tds::codec::Encode};
use bytes::BytesMut;

#[derive(Debug)]
pub(crate) struct TokenSspi(Vec<u8>);

impl AsRef<[u8]> for TokenSspi {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl TokenSspi {
    #[cfg(windows)]
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub(crate) fn decode_async<R>(src: &mut R) -> crate::mssql::connection::Result<Self>
    where
        R: SqlReadBytes,
    {
        let len = src.read_u16_le()? as usize;
        let mut bytes = vec![0; len];
        src.read_exact(&mut bytes[0..len])?;

        Ok(Self(bytes))
    }
}

impl Encode<BytesMut> for TokenSspi {
    fn encode(self, dst: &mut BytesMut) -> crate::mssql::connection::Result<()> {
        dst.extend(self.0);
        Ok(())
    }
}
