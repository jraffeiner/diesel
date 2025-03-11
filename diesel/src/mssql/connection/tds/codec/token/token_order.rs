use crate::mssql::connection::SqlReadBytes;

#[allow(dead_code)] // we might want to debug the values
#[derive(Debug)]
pub(crate) struct TokenOrder {
    pub(crate) column_indexes: Vec<u16>,
}

impl TokenOrder {
    pub(crate) fn decode<R>(src: &mut R) -> crate::mssql::connection::Result<Self>
    where
        R: SqlReadBytes,
    {
        let len = src.read_u16_le()? / 2;

        let mut column_indexes = Vec::with_capacity(len as usize);

        for _ in 0..len {
            column_indexes.push(src.read_u16_le()?);
        }

        Ok(TokenOrder { column_indexes })
    }
}
