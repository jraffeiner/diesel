use crate::mssql::connection::{SqlReadBytes, TypeInfo};

use super::ColumnData;

pub(crate) fn decode<R>(src: &mut R) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let ctx = TypeInfo::decode(src)?;
    let cd = ColumnData::decode(src, &ctx)?;
    Ok(cd)
}
