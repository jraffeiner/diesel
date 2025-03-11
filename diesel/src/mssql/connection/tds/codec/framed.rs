use std::{
    io::{self, Error, ErrorKind, Read, Write},
    ops::{Deref, DerefMut},
};

use bytes::{Buf, BytesMut};

use super::{sink::Sink, Decoder, Encoder};

pub(crate) struct Framed<S, C> {
    inner: FramedReadBuffer<FramedWriteBuffer<Fuse<S, C>>>,
}

impl<S, C> Deref for Framed<S, C> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S, C> DerefMut for Framed<S, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<S, C> Framed<S, C>
where
    S: Read + Write,
    C: Decoder + Encoder,
{
    pub(crate) fn new(inner: S, codec: C) -> Self {
        let inner =
            FramedReadBuffer::new(FramedWriteBuffer::new(Fuse::new(inner, codec), None), None);
        Self { inner }
    }

    /// Creates a new `Framed` from [`FramedParts`].
    ///
    /// See also [`Framed::into_parts`].
    #[allow(dead_code)]
    pub(crate) fn from_parts(
        FramedParts {
            io,
            codec,
            write_buffer,
            read_buffer,
            ..
        }: FramedParts<S, C>,
    ) -> Self {
        let framed_write = FramedWriteBuffer::new(Fuse::new(io, codec), Some(write_buffer));
        let framed_read = FramedReadBuffer::new(framed_write, Some(read_buffer));
        Self { inner: framed_read }
    }

    /// Consumes the `Framed`, returning its parts, such that a new
    /// `Framed` may be constructed, possibly with a different codec.
    ///
    /// See also [`Framed::from_parts`].
    pub(crate) fn into_parts(self) -> FramedParts<S, C> {
        let (framed_write, read_buffer) = self.inner.into_parts();
        let (fuse, write_buffer) = framed_write.into_parts();
        FramedParts {
            io: fuse.s,
            codec: fuse.c,
            read_buffer,
            write_buffer,
        }
    }

    pub(crate) fn into_inner(self) -> S {
        self.into_parts().io
    }

    #[allow(dead_code)]
    pub(crate) fn codec(&self) -> &C {
        &self.inner.c
    }

    #[allow(dead_code)]
    pub(crate) fn codec_mut(&mut self) -> &mut C {
        &mut self.inner.c
    }

    #[allow(dead_code)]
    pub(crate) fn read_buffer(&self) -> &BytesMut {
        self.inner.buffer()
    }

    /// High-water mark for writes, in bytes
    ///
    /// See [`FramedWrite::send_high_water_mark`].
    #[allow(dead_code)]
    pub(crate) fn send_high_water_mark(&self) -> usize {
        self.inner.high_water_mark
    }

    /// Sets high-water mark for writes, in bytes
    ///
    /// See [`FramedWrite::set_send_high_water_mark`].
    #[allow(dead_code)]
    pub(crate) fn set_send_high_water_mark(&mut self, hwm: usize) {
        self.inner.high_water_mark = hwm;
    }
}

impl<S, C> Iterator for Framed<S, C>
where
    S: Read,
    C: Decoder,
{
    type Item = Result<C::Item, C::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<S, C> Sink<C::Item<'_>> for Framed<S, C>
where
    S: Write,
    C: Encoder,
{
    type Error = C::Error;

    fn send(&mut self, item: C::Item<'_>) -> Result<(), Self::Error> {
        self.inner.send(item)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Sink::flush(&mut self.inner)
    }
}
/*
impl<S:Read,C> Read for Framed<S,C>{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<S:Write,C> Write for Framed<S,C>{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
*/

#[derive(Debug)]
struct FramedReadBuffer<S> {
    inner: S,
    buffer: BytesMut,
}

const INITIAL_CAPACITY: usize = 8 * 1024;

impl<S> FramedReadBuffer<S> {
    fn new(inner: S, buffer: Option<BytesMut>) -> Self {
        Self {
            inner,
            buffer: buffer.unwrap_or_else(|| BytesMut::with_capacity(INITIAL_CAPACITY)),
        }
    }

    fn into_parts(self) -> (S, BytesMut) {
        (self.inner, self.buffer)
    }

    #[allow(dead_code)]
    fn buffer(&self) -> &BytesMut {
        &self.buffer
    }
}

impl<S> Deref for FramedReadBuffer<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S> DerefMut for FramedReadBuffer<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<S> Iterator for FramedReadBuffer<S>
where
    S: Read + Decoder,
{
    type Item = Result<S::Item, S::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.inner.decode(&mut self.buffer).transpose() {
            return Some(item);
        }

        let mut buf = [0; INITIAL_CAPACITY];

        loop {
            let n = match self.inner.read(&mut buf) {
                Ok(n) => n,
                Err(e) => return Some(Err(e.into())),
            };
            self.buffer.extend_from_slice(&buf[..n]);

            let ended = n == 0;

            match self.inner.decode(&mut self.buffer).transpose() {
                Some(item) => return Some(item),
                None if ended => {
                    if self.buffer.is_empty() {
                        return None;
                    } else {
                        match self.inner.decode_eof(&mut self.buffer).transpose() {
                            Some(item) => return Some(item),
                            None if self.buffer.is_empty() => return None,
                            None => {
                                return Some(Err(io::Error::new(
                                    io::ErrorKind::UnexpectedEof,
                                    "bytes remaining in stream",
                                )
                                .into()))
                            }
                        }
                    }
                }
                _ => continue,
            }
        }
    }
}

impl<S, I> Sink<I> for FramedReadBuffer<S>
where
    S: Sink<I>,
{
    type Error = S::Error;

    fn send(&mut self, item: I) -> Result<(), Self::Error> {
        self.inner.send(item)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.inner.flush()
    }
}

impl<S: Write> Write for FramedReadBuffer<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<S: Read> Read for FramedReadBuffer<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

struct Fuse<S, C> {
    s: S,
    c: C,
}

impl<S, C> Fuse<S, C> {
    fn new(s: S, c: C) -> Self {
        Self { s, c }
    }
}

impl<S, C> Deref for Fuse<S, C> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.s
    }
}

impl<S, C> DerefMut for Fuse<S, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.s
    }
}

impl<S: Read, C> Read for Fuse<S, C> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.s.read(buf)
    }
}

impl<S: Write, C> Write for Fuse<S, C> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.s.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.s.flush()
    }
}

struct FramedWriteBuffer<S> {
    inner: S,
    high_water_mark: usize,
    buffer: BytesMut,
}

impl<S> Deref for FramedWriteBuffer<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S> DerefMut for FramedWriteBuffer<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

const DEFAULT_SEND_HIGH_WATER_MARK: usize = 131072;

impl<S> FramedWriteBuffer<S> {
    fn new(inner: S, buffer: Option<BytesMut>) -> Self {
        Self {
            inner,
            high_water_mark: DEFAULT_SEND_HIGH_WATER_MARK,
            buffer: buffer.unwrap_or_else(|| BytesMut::with_capacity(INITIAL_CAPACITY)),
        }
    }

    fn into_parts(self) -> (S, BytesMut) {
        (self.inner, self.buffer)
    }
}

impl<S: Read> Read for FramedWriteBuffer<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<S> Sink<S::Item<'_>> for FramedWriteBuffer<S>
where
    S: Write + Encoder,
{
    type Error = S::Error;

    fn send(&mut self, item: S::Item<'_>) -> Result<(), Self::Error> {
        while self.buffer.len() >= self.high_water_mark {
            let num_write = self.inner.write(&self.buffer)?;
            if num_write == 0 {
                return Err(Error::new(ErrorKind::UnexpectedEof, "End of file").into());
            }

            self.buffer.advance(num_write);
        }
        self.inner.encode(item, &mut self.buffer)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        while !self.buffer.is_empty() {
            let num_write = self.inner.write(&self.buffer)?;
            if num_write == 0 {
                return Err(Error::new(ErrorKind::UnexpectedEof, "End of file").into());
            }

            self.buffer.advance(num_write);
        }
        self.inner.flush().map_err(Into::into)
    }
}

impl<S: Iterator> Iterator for FramedWriteBuffer<S> {
    type Item = S::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<S: Write> Write for FramedWriteBuffer<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[allow(dead_code)]
pub(crate) struct FramedParts<S, C> {
    io: S,
    codec: C,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
}

impl<S, C: Decoder> Decoder for Fuse<S, C> {
    type Item = C::Item;

    type Error = C::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.c.decode(src)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.c.decode_eof(src)
    }
}

impl<S: Decoder> Decoder for FramedWriteBuffer<S> {
    type Item = S::Item;

    type Error = S::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode_eof(src)
    }
}

impl<S: Encoder> Encoder for FramedReadBuffer<S> {
    type Item<'a> = S::Item<'a>;

    type Error = S::Error;

    fn encode(&mut self, item: Self::Item<'_>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

impl<S, C: Encoder> Encoder for Fuse<S, C> {
    type Item<'a> = C::Item<'a>;

    type Error = C::Error;

    fn encode(&mut self, item: Self::Item<'_>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.c.encode(item, dst)
    }
}
