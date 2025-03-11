use crate::mssql::connection::{sql_read_bytes::SqlReadBytes, time::DateTimeOffset, ColumnData};

pub(crate) fn decode<R>(
    src: &mut R,
    len: usize,
) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let rlen = src.read_u8()?;

    let dto = match rlen {
        0 => ColumnData::DateTimeOffset(None),
        _ => {
            let dto = DateTimeOffset::decode(src, len, rlen - 5)?;
            ColumnData::DateTimeOffset(Some(dto))
        }
    };

    Ok(dto)
}
