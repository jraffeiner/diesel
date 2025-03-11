use crate::mssql::connection::Error;

use crate::mssql::connection::{sql_read_bytes::SqlReadBytes, time::Date, ColumnData};

pub(crate) fn decode<R>(src: &mut R) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let len = src.read_u8()?;

    let res = match len {
        0 => ColumnData::Date(None),
        3 => ColumnData::Date(Some(Date::decode(src)?)),
        _ => {
            return Err(Error::Protocol(
                format!("daten: length of {} is invalid", len).into(),
            ))
        }
    };

    Ok(res)
}
