use crate::mssql::connection::{
    sql_read_bytes::SqlReadBytes, tds::codec::VarLenContext, ColumnData, VarLenType,
};

pub(crate) fn decode<R>(
    src: &mut R,
    ctx: &VarLenContext,
) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    use VarLenType::*;

    let ty = ctx.r#type();
    let len = ctx.len();
    let collation = ctx.collation();

    let res = match ty {
        Bitn => super::bit::decode(src)?,
        Intn => super::int::decode(src, len)?,
        Floatn => super::float::decode(src, len)?,
        Guid => super::guid::decode(src)?,
        BigChar | BigVarChar | NChar | NVarchar => {
            ColumnData::String(super::string::decode(src, ty, len, collation)?)
        }
        Money => {
            let len = src.read_u8()?;
            super::money::decode(src, len)?
        }
        Datetimen => {
            let rlen = src.read_u8()?;
            super::datetimen::decode(src, rlen, len as u8)?
        }
        Daten => super::date::decode(src)?,
        Timen => super::time::decode(src, len)?,
        Datetime2 => super::datetime2::decode(src, len)?,
        DatetimeOffsetn => super::datetimeoffsetn::decode(src, len)?,
        BigBinary | BigVarBin => super::binary::decode(src, len)?,
        Text => super::text::decode(src, collation)?,
        NText => super::text::decode(src, None)?,
        Image => super::image::decode(src)?,
        SSVariant => super::sql_variant::decode(src)?,
        t => unimplemented!("{:?}", t),
    };

    Ok(res)
}
