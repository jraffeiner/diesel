use uuid::Uuid;

use crate::mssql::connection::{
    error::Error, sql_read_bytes::SqlReadBytes, tds::codec::guid, ColumnData,
};

pub(crate) fn decode<R>(src: &mut R) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let len = src.read_u8()? as usize;

    let res = match len {
        0 => ColumnData::Guid(None),
        16 => {
            let mut data = [0u8; 16];

            for item in &mut data {
                *item = src.read_u8()?;
            }

            guid::reorder_bytes(&mut data);
            ColumnData::Guid(Some(Uuid::from_bytes(data)))
        }
        _ => {
            return Err(Error::Protocol(
                format!("guid: length of {} is invalid", len).into(),
            ))
        }
    };

    Ok(res)
}
