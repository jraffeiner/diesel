use super::BaseMetaDataColumn;
use crate::mssql::connection::{tds::codec::ColumnData, Error, SqlReadBytes};

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct TokenReturnValue {
    pub param_ordinal: u16,
    pub param_name: String,
    /// return value of user defined function
    pub udf: bool,
    pub meta: BaseMetaDataColumn,
    pub value: ColumnData<'static>,
}

impl TokenReturnValue {
    pub(crate) fn decode<R>(src: &mut R) -> crate::mssql::connection::Result<Self>
    where
        R: SqlReadBytes,
    {
        let param_ordinal = src.read_u16_le()?;
        let param_name = src.read_b_varchar()?;

        let udf = match src.read_u8()? {
            0x01 => false,
            0x02 => true,
            _ => return Err(Error::Protocol("ReturnValue: invalid status".into())),
        };

        let meta = BaseMetaDataColumn::decode(src)?;
        let value = ColumnData::decode(src, &meta.ty)?;

        let token = TokenReturnValue {
            param_ordinal,
            param_name,
            udf,
            meta,
            value,
        };

        Ok(token)
    }
}
