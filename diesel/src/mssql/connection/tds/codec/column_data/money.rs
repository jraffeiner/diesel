use crate::mssql::connection::{error::Error, sql_read_bytes::SqlReadBytes, ColumnData};

pub(crate) fn decode<R>(
    src: &mut R,
    len: u8,
) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let res = match len {
        0 => ColumnData::F64(None),
        4 => ColumnData::F64(Some(src.read_i32_le()? as f64 / 1e4)),
        8 => ColumnData::F64(Some({
            let high = src.read_i32_le()? as i64;
            let low = src.read_u32_le()? as f64;

            ((high << 32) as f64 + low) / 1e4
        })),
        _ => {
            return Err(Error::Protocol(
                format!("money: length of {} is invalid", len).into(),
            ))
        }
    };

    Ok(res)
}
