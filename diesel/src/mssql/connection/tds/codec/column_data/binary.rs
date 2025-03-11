use std::borrow::Cow;

use crate::mssql::connection::{sql_read_bytes::SqlReadBytes, ColumnData};

pub(crate) fn decode<R>(
    src: &mut R,
    len: usize,
) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let data = super::plp::decode(src, len)?.map(Cow::from);

    Ok(ColumnData::Binary(data))
}
