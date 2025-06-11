use crate::mssql::connection::SqlReadBytes;

#[expect(dead_code)] // we might want to debug the values
#[derive(Debug)]
pub(crate) struct TokenInfo {
    /// info number
    pub(crate) number: u32,
    /// error state
    pub(crate) state: u8,
    /// severity (<10: Info)
    pub(crate) class: u8,
    pub(crate) message: String,
    pub(crate) server: String,
    pub(crate) procedure: String,
    pub(crate) line: u32,
}

impl TokenInfo {
    pub(crate) fn decode<R>(src: &mut R) -> crate::mssql::connection::Result<Self>
    where
        R: SqlReadBytes,
    {
        let _length = src.read_u16_le()?;

        let number = src.read_u32_le()?;
        let state = src.read_u8()?;
        let class = src.read_u8()?;
        let message = src.read_us_varchar()?;
        let server = src.read_b_varchar()?;
        let procedure = src.read_b_varchar()?;
        let line = src.read_u32_le()?;

        Ok(TokenInfo {
            number,
            state,
            class,
            message,
            server,
            procedure,
            line,
        })
    }
}
