use crate::mssql::connection::{error::Error, sql_read_bytes::SqlReadBytes, ColumnData};

pub(crate) fn decode<R>(src: &mut R) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let recv_len = src.read_u8()? as usize;

    let res = match recv_len {
        0 => ColumnData::Bit(None),
        1 => ColumnData::Bit(Some(src.read_u8()? > 0)),
        v => {
            return Err(Error::Protocol(
                format!("bitn: length of {} is invalid", v).into(),
            ))
        }
    };

    Ok(res)
}
