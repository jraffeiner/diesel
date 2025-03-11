use crate::mssql::connection::{sql_read_bytes::SqlReadBytes, ColumnData};

pub(crate) fn decode<R>(src: &mut R) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let ptr_len = src.read_u8()? as usize;

    if ptr_len == 0 {
        return Ok(ColumnData::Binary(None));
    }

    for _ in 0..ptr_len {
        src.read_u8()?;
    }

    src.read_i32_le()?; // days
    src.read_u32_le()?; // second fractions

    let len = src.read_u32_le()? as usize;
    let mut buf = Vec::with_capacity(len);

    for _ in 0..len {
        buf.push(src.read_u8()?);
    }

    Ok(ColumnData::Binary(Some(buf.into())))
}
