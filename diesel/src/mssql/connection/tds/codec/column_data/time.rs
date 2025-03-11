use crate::mssql::connection::{sql_read_bytes::SqlReadBytes, time::Time, ColumnData};

pub(crate) fn decode<R>(
    src: &mut R,
    len: usize,
) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let rlen = src.read_u8()?;

    let time = match rlen {
        0 => ColumnData::Time(None),
        _ => {
            let time = Time::decode(src, len, rlen as usize)?;
            ColumnData::Time(Some(time))
        }
    };

    Ok(time)
}
