use super::{AllHeaderTy, Encode, ALL_HEADERS_LEN_TX};
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

//#[expect(dead_code)]
pub(crate) struct BatchRequest<'a> {
    queries: Cow<'a, str>,
    transaction_descriptor: [u8; 8],
}

impl<'a> BatchRequest<'a> {
    //#[expect(dead_code)]
    pub(crate) fn new(queries: impl Into<Cow<'a, str>>, transaction_descriptor: [u8; 8]) -> Self {
        Self {
            queries: queries.into(),
            transaction_descriptor,
        }
    }
}

impl<'a> Encode<BytesMut> for BatchRequest<'a> {
    fn encode(self, dst: &mut BytesMut) -> crate::mssql::connection::Result<()> {
        dst.put_u32_le(ALL_HEADERS_LEN_TX as u32);
        dst.put_u32_le(ALL_HEADERS_LEN_TX as u32 - 4);
        dst.put_u16_le(AllHeaderTy::TransactionDescriptor as u16);
        dst.put_slice(&self.transaction_descriptor);
        dst.put_u32_le(1);

        for c in self.queries.encode_utf16() {
            dst.put_u16_le(c);
        }

        Ok(())
    }
}
