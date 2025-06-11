use bytes::BytesMut;
use std::io::{Read, Write};
use tracing::{event, Level};

use crate::mssql::connection::{
    client::Connection, sql_read_bytes::SqlReadBytes, BytesMutWithDataColumns, ExecuteResult,
};

use super::{
    Encode, MetaDataColumn, PacketHeader, PacketStatus, TokenColMetaData, TokenDone, TokenRow,
    HEADER_BYTES,
};

/// A handler for a bulk insert data flow.
#[derive(Debug)]
//#[expect(dead_code)]
pub(crate) struct BulkLoadRequest<'a, S>
where
    S: Read + Write + Send,
{
    connection: &'a mut Connection<S>,
    packet_id: u8,
    buf: BytesMut,
    columns: Vec<MetaDataColumn<'a>>,
}

impl<'a, S> BulkLoadRequest<'a, S>
where
    S: Read + Write + Send,
{
    //#[expect(dead_code)]
    pub(crate) fn new(
        connection: &'a mut Connection<S>,
        columns: Vec<MetaDataColumn<'a>>,
    ) -> crate::mssql::connection::Result<Self> {
        let packet_id = connection.context_mut().next_packet_id();
        let mut buf = BytesMut::new();

        let cmd = TokenColMetaData {
            columns: columns.clone(),
        };

        cmd.encode(&mut buf)?;

        let this = Self {
            connection,
            packet_id,
            buf,
            columns,
        };

        Ok(this)
    }

    /// Adds a new row to the bulk insert, flushing only when having a full packet of data.
    ///
    /// # Warning
    ///
    /// After the last row, [`finalize`] must be called to flush the buffered
    /// data and for the data to actually be available in the table.
    ///
    /// [`finalize`]: #method.finalize
    #[expect(dead_code)]
    pub(crate) fn send(&mut self, row: TokenRow<'a>) -> crate::mssql::connection::Result<()> {
        let mut buf_with_columns = BytesMutWithDataColumns::new(&mut self.buf, &self.columns);

        row.encode(&mut buf_with_columns)?;
        self.write_packets()?;

        Ok(())
    }

    /// Ends the bulk load, flushing all pending data to the wire.
    ///
    /// This method must be called after sending all the data to flush all
    /// pending data and to get the server actually to store the rows to the
    /// table.
    #[expect(dead_code)]
    pub(crate) fn finalize(mut self) -> crate::mssql::connection::Result<ExecuteResult> {
        TokenDone::default().encode(&mut self.buf)?;
        self.write_packets()?;

        let mut header = PacketHeader::bulk_load(self.packet_id);
        header.set_status(PacketStatus::EndOfMessage);

        let data = self.buf.split();

        event!(
            Level::TRACE,
            "Finalizing a bulk insert ({} bytes)",
            data.len() + HEADER_BYTES,
        );

        self.connection.write_to_wire(header, data)?;
        self.connection.flush_sink()?;

        ExecuteResult::new(self.connection)
    }

    fn write_packets(&mut self) -> crate::mssql::connection::Result<()> {
        let packet_size = (self.connection.context().packet_size() as usize) - HEADER_BYTES;

        while self.buf.len() > packet_size {
            let header = PacketHeader::bulk_load(self.packet_id);
            let data = self.buf.split_to(packet_size);

            event!(
                Level::TRACE,
                "Bulk insert packet ({} bytes)",
                data.len() + HEADER_BYTES,
            );

            self.connection.write_to_wire(header, data)?;
        }

        Ok(())
    }
}
