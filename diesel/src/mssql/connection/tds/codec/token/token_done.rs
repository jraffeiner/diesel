use crate::mssql::connection::{tds::codec::Encode, Error, SqlReadBytes, TokenType};
use bytes::BufMut;
use bytes::BytesMut;
use enumflags2::{bitflags, BitFlags};
use std::fmt;

#[derive(Debug, Default)]
pub(crate) struct TokenDone {
    status: BitFlags<DoneStatus>,
    cur_cmd: u16,
    done_rows: u64,
}

#[bitflags]
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoneStatus {
    More = 1 << 0,
    Error = 1 << 1,
    Inexact = 1 << 2,
    // reserved
    Count = 1 << 4,
    Attention = 1 << 5,
    // reserved
    RpcInBatch = 1 << 7,
    SrvError = 1 << 8,
}

impl TokenDone {
    pub(crate) fn decode<R>(src: &mut R) -> crate::mssql::connection::Result<Self>
    where
        R: SqlReadBytes,
    {
        let status = BitFlags::from_bits(src.read_u16_le()?)
            .map_err(|_| Error::Protocol("done(variant): invalid status".into()))?;

        let cur_cmd = src.read_u16_le()?;
        let done_row_count_bytes = src.context().version().done_row_count_bytes();

        let done_rows = match done_row_count_bytes {
            8 => src.read_u64_le()?,
            4 => src.read_u32_le()? as u64,
            _ => unreachable!(),
        };

        Ok(TokenDone {
            status,
            cur_cmd,
            done_rows,
        })
    }

    pub(crate) fn is_final(&self) -> bool {
        self.status.is_empty()
    }

    pub(crate) fn rows(&self) -> u64 {
        self.done_rows
    }
}

impl Encode<BytesMut> for TokenDone {
    fn encode(self, dst: &mut BytesMut) -> crate::mssql::connection::Result<()> {
        dst.put_u8(TokenType::Done as u8);
        dst.put_u16_le(BitFlags::bits(self.status));

        dst.put_u16_le(self.cur_cmd);
        dst.put_u64_le(self.done_rows);

        Ok(())
    }
}

impl fmt::Display for TokenDone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.done_rows == 0 {
            write!(f, "Done with status {:?}", self.status)
        } else if self.done_rows == 1 {
            write!(f, "Done with status {:?} (1 row left)", self.status)
        } else {
            write!(
                f,
                "Done with status {:?} ({} rows left)",
                self.status, self.done_rows
            )
        }
    }
}
