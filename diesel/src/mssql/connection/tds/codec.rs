mod batch_request;
mod bulk_load;
mod column_data;
mod decode;
mod encode;
pub(crate) mod framed;
mod guid;
mod header;
mod iterator_ext;
mod login;
mod packet;
mod pre_login;
mod rpc_request;
pub(crate) mod sink;
mod token;
mod type_info;

use std::io::Error;

pub(crate) use batch_request::*;
pub(crate) use bulk_load::*;
use bytes::BytesMut;
pub(crate) use column_data::*;
pub(crate) use decode::*;
pub(crate) use encode::*;
pub(crate) use header::*;
pub(crate) use iterator_ext::*;
pub(crate) use login::*;
pub(crate) use packet::*;
pub(crate) use pre_login::*;
pub(crate) use rpc_request::*;
pub(crate) use token::*;
pub(crate) use type_info::*;

pub use column_data::ColumnData;

const HEADER_BYTES: usize = 8;
const ALL_HEADERS_LEN_TX: usize = 22;

#[derive(Debug)]
#[repr(u16)]
#[expect(dead_code)]
enum AllHeaderTy {
    QueryDescriptor = 1,
    TransactionDescriptor = 2,
    TraceActivity = 3,
}

/// Encoding of messages as bytes, for use with `FramedWrite`.
pub(crate) trait Encoder {
    /// The type of items consumed by `encode`
    type Item<'a>;
    /// The type of encoding errors.
    type Error: From<Error>;

    /// Encodes an item into the `BytesMut` provided by dst.
    fn encode(&mut self, item: Self::Item<'_>, dst: &mut BytesMut) -> Result<(), Self::Error>;
}

/// Decoding of frames via buffers, for use with `FramedRead`.
pub(crate) trait Decoder {
    /// The type of items returned by `decode`
    type Item;
    /// The type of decoding errors.
    type Error: From<Error>;

    /// Decode an item from the src `BytesMut` into an item
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error>;

    /// Called when the input stream reaches EOF, signaling a last attempt to decode
    ///
    /// # Notes
    ///
    /// The default implementation of this method invokes the `Decoder::decode` method.
    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode(src)
    }
}

pub(crate) struct PacketCodec;

pub(crate) fn collect_from<S, T>(stream: &mut S) -> crate::mssql::connection::Result<T>
where
    T: Decode<BytesMut> + Sized,
    S: Iterator<Item = crate::mssql::connection::Result<Packet>>,
{
    let mut buf = BytesMut::new();

    for packet in stream.by_ref() {
        let packet = packet?;
        let is_last = packet.is_last();
        let (_, payload) = packet.into_parts();
        buf.extend(payload);

        if is_last {
            break;
        }
    }

    T::decode(&mut buf)
}
