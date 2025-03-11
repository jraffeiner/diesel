use crate::mssql::connection::{sql_read_bytes::SqlReadBytes, time::DateTime2, ColumnData};

pub(crate) fn decode<R>(
    src: &mut R,
    len: usize,
) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let rlen = src.read_u8()?;

    let date = match rlen {
        0 => ColumnData::DateTime2(None),
        rlen => {
            let dt = DateTime2::decode(src, len, rlen as usize - 3)?;
            ColumnData::DateTime2(Some(dt))
        }
    };

    Ok(date)
}
