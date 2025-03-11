use std::{borrow::Cow, sync::Arc};

use crate::mssql::connection::{
    sql_read_bytes::SqlReadBytes,
    xml::{XmlData, XmlSchema},
    ColumnData, VarLenType,
};

pub(crate) fn decode<R>(
    src: &mut R,
    len: usize,
    schema: Option<Arc<XmlSchema>>,
) -> crate::mssql::connection::Result<ColumnData<'static>>
where
    R: SqlReadBytes,
{
    let xml = super::string::decode(src, VarLenType::Xml, len, None)?.map(|data| {
        let mut data = XmlData::new(data);

        if let Some(schema) = schema {
            data.set_schema(schema);
        }

        Cow::Owned(data)
    });

    Ok(ColumnData::Xml(xml))
}
