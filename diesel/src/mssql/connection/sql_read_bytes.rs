use crate::mssql::connection::tds::Context;
use bytes::Buf;
use std::io;
use std::io::Read;

/// The `SqlReadBytes` trait is used to read bytes from the wire.
// Many of the methods have an `expect(dead_code)` attribute because they are not currently used but they could be anytime in the future.
pub(crate) trait SqlReadBytes: Read {
    // Pretty-print current wire content.
    #[expect(dead_code)]
    fn debug_buffer(&self);

    // The client state.
    fn context(&self) -> &Context;

    // A mutable reference to the SQL client state.
    fn context_mut(&mut self) -> &mut Context;

    // Read a single i8 value.
    #[expect(dead_code)]
    fn read_i8(&mut self) -> io::Result<i8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        let res = Buf::get_i8(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single byte value.
    fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        let res = Buf::get_u8(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single u16 value.
    fn read_u16_le(&mut self) -> io::Result<u16> {
        let mut buf = [0; 2];
        self.read_exact(&mut buf)?;
        let res = Buf::get_u16_le(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single i16 value.
    fn read_i16_le(&mut self) -> io::Result<i16> {
        let mut buf = [0; 2];
        self.read_exact(&mut buf)?;
        let res = Buf::get_i16_le(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single big-endian f32 value.
    #[expect(dead_code)]
    fn read_f32(&mut self) -> io::Result<f32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        let res = Buf::get_f32(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single big-endian u32 value.
    fn read_u32(&mut self) -> io::Result<u32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        let res = Buf::get_u32(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single f32 value.
    fn read_f32_le(&mut self) -> io::Result<f32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        let res = Buf::get_f32_le(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single i32 value.
    fn read_i32_le(&mut self) -> io::Result<i32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        let res = Buf::get_i32_le(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single u32 value.
    fn read_u32_le(&mut self) -> io::Result<u32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        let res = Buf::get_u32_le(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single big-endian f64 value.
    #[expect(dead_code)]
    fn read_f64(&mut self) -> io::Result<f64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        let res = Buf::get_f64(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single f64 value.
    fn read_f64_le(&mut self) -> io::Result<f64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        let res = Buf::get_f64_le(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single i64 value.
    fn read_i64_le(&mut self) -> io::Result<i64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        let res = Buf::get_i64_le(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single u64 value.
    fn read_u64_le(&mut self) -> io::Result<u64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        let res = Buf::get_u64_le(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single i128 value.
    #[expect(dead_code)]
    fn read_i128_le(&mut self) -> io::Result<i128> {
        let mut buf = [0; 16];
        self.read_exact(&mut buf)?;
        let res = Buf::get_i128_le(&mut buf.as_slice());
        Ok(res)
    }

    // Read a single u128 value.
    #[expect(dead_code)]
    fn read_u128_le(&mut self) -> io::Result<u128> {
        let mut buf = [0; 16];
        self.read_exact(&mut buf)?;
        let res = Buf::get_u128_le(&mut buf.as_slice());
        Ok(res)
    }

    // A variable-length character stream defined by a length-field of an u8.
    fn read_b_varchar(&mut self) -> io::Result<String> {
        let len = self.read_u8()? as usize;
        let mut buf = Vec::<u16>::with_capacity(len);

        while buf.len() < len {
            let val = self.read_u16_le()?;
            buf.push(val)
        }

        // Everything's read, we can return the string.
        let s = String::from_utf16(&buf)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-16 data."))?;
        Ok(s)
    }

    // A variable-length character stream defined by a length-field of an u16.
    fn read_us_varchar(&mut self) -> io::Result<String> {
        let len = self.read_u16_le()? as usize;
        let mut buf = Vec::<u16>::with_capacity(len);

        while buf.len() < len {
            let val = self.read_u16_le()?;
            buf.push(val)
        }

        // Everything's read, we can return the string.
        let s = String::from_utf16(&buf)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-16 data."))?;
        Ok(s)
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use crate::mssql::connection::tds::Context;
    use crate::mssql::connection::SqlReadBytes;
    use bytes::BytesMut;
    use std::io;
    use std::io::Read;

    // a test util to run decode logic on BytesMut, for testing loop back
    pub(crate) trait IntoSqlReadBytes {
        type T: SqlReadBytes;
        fn into_sql_read_bytes(self) -> Self::T;
    }

    impl IntoSqlReadBytes for BytesMut {
        type T = BytesMutReader;

        fn into_sql_read_bytes(self) -> Self::T {
            BytesMutReader { buf: self }
        }
    }

    pub(crate) struct BytesMutReader {
        buf: BytesMut,
    }

    impl Read for BytesMutReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let size = buf.len();

            if self.buf.len() < size {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "No more packets in the wire",
                ));
            }

            buf.copy_from_slice(self.buf.split_to(size).as_ref());
            Ok(size)
        }
    }

    impl SqlReadBytes for BytesMutReader {
        fn debug_buffer(&self) {
            todo!()
        }

        fn context(&self) -> &Context {
            todo!()
        }

        fn context_mut(&mut self) -> &mut Context {
            todo!()
        }
    }
}
