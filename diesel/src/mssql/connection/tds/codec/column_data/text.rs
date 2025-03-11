use crate::mssql::connection::{
    error::Error, sql_read_bytes::SqlReadBytes, tds::Collation, ColumnData,
};

pub(crate) fn decode<R>(
    src: &mut R,
    collation: Option<Collation>,
) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let ptr_len = src.read_u8()? as usize;

    if ptr_len == 0 {
        return Ok(ColumnData::String(None));
    }

    for _ in 0..ptr_len {
        src.read_u8()?;
    }

    src.read_i32_le()?; // days
    src.read_u32_le()?; // second fractions

    let text = match collation {
        // TEXT
        Some(collation) => {
            let encoder = collation.encoding()?;
            let text_len = src.read_u32_le()? as usize;
            let mut buf = Vec::with_capacity(text_len);

            for _ in 0..text_len {
                buf.push(src.read_u8()?);
            }

            encoder
                .decode_without_bom_handling_and_without_replacement(buf.as_ref())
                .ok_or_else(|| Error::Encoding("invalid sequence".into()))?
                .to_string()
        }
        // NTEXT
        None => {
            let text_len = src.read_u32_le()? as usize / 2;
            let mut buf = Vec::with_capacity(text_len);

            for _ in 0..text_len {
                buf.push(src.read_u16_le()?);
            }

            String::from_utf16(&buf[..])?
        }
    };

    Ok(ColumnData::String(Some(text.into())))
}
