use super::{Encoder, Packet, PacketCodec};
use bytes::{BufMut, BytesMut};

pub(crate) trait Encode<B: BufMut> {
    fn encode(self, dst: &mut B) -> crate::mssql::connection::Result<()>;
}

impl Encoder for PacketCodec {
    type Item<'a> = Packet;
    type Error = crate::mssql::connection::Error;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst)?;
        Ok(())
    }
}
