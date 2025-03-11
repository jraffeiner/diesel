use crate::mssql::connection::{sql_read_bytes::SqlReadBytes, ColumnData, FixedLenType};

pub(crate) fn decode<R>(
    src: &mut R,
    r#type: &FixedLenType,
) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let data = match r#type {
        FixedLenType::Null => ColumnData::Bit(None),
        FixedLenType::Bit => ColumnData::Bit(Some(src.read_u8()? != 0)),
        FixedLenType::Int1 => ColumnData::U8(Some(src.read_u8()?)),
        FixedLenType::Int2 => ColumnData::I16(Some(src.read_i16_le()?)),
        FixedLenType::Int4 => ColumnData::I32(Some(src.read_i32_le()?)),
        FixedLenType::Int8 => ColumnData::I64(Some(src.read_i64_le()?)),
        FixedLenType::Float4 => ColumnData::F32(Some(src.read_f32_le()?)),
        FixedLenType::Float8 => ColumnData::F64(Some(src.read_f64_le()?)),
        FixedLenType::Datetime => super::datetimen::decode(src, 8, 8)?,
        FixedLenType::Datetime4 => super::datetimen::decode(src, 4, 8)?,
        FixedLenType::Money4 => super::money::decode(src, 4)?,
        FixedLenType::Money => super::money::decode(src, 8)?,
    };

    Ok(data)
}
