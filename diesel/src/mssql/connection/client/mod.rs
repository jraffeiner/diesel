mod auth;
mod config;
mod connection;

mod tls;
#[cfg(any(feature = "rustls", feature = "native-tls",))]
mod tls_stream;

pub(crate) use auth::*;
pub(crate) use config::*;
pub(crate) use connection::*;

use super::tds::stream::ReceivedToken;
use super::to_sql::ToSql;
use super::{
    result::ExecuteResult,
    tds::{
        codec::{self, IteratorJoin},
        stream::{QueryStream, TokenStream},
    },
    BulkLoadRequest, ColumnFlag, SqlReadBytes,
};
use codec::{BatchRequest, ColumnData, PacketHeader, RpcParam, RpcProcId, TokenRpcRequest};
use enumflags2::BitFlags;
use std::io::{Read, Write};
use std::{borrow::Cow, fmt::Debug};

/// `Client` is the main entry point to the SQL Server, providing query
/// execution capabilities.
///
/// A `Client` is created using the [`Config`], defining the needed
/// connection options and capabilities.
///
/// # Example
///
/// ```no_run
/// # use tiberius::{Config, AuthMethod};
///
/// #
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut config = Config::new();
///
/// config.host("0.0.0.0");
/// config.port(1433);
/// config.authentication(AuthMethod::sql_server("SA", "<Mys3cureP4ssW0rD>"));
///
/// let tcp = std::net::TcpStream::connect(config.get_addr())?;
/// tcp.set_nodelay(true)?;
/// // Client is ready to use.
/// let client = tiberius::Client::connect(config, tcp)?;
/// # Ok(())
/// # }
/// ```
///
/// [`Config`]: struct.Config.html
#[derive(Debug)]
pub(crate) struct Client<S: Read + Write + Send> {
    pub(crate) connection: Connection<S>,
}

impl<S: Read + Write + Send> Client<S> {
    /// Uses an instance of [`Config`] to specify the connection
    /// options required to connect to the database using an established
    /// tcp connection
    ///
    /// [`Config`]: struct.Config.html
    pub(crate) fn connect(
        config: Config,
        tcp_stream: S,
    ) -> crate::mssql::connection::Result<Client<S>> {
        Ok(Client {
            connection: Connection::connect(config, tcp_stream)?,
        })
    }

    /// Executes SQL statements in the SQL Server, returning the number rows
    /// affected. Useful for `INSERT`, `UPDATE` and `DELETE` statements. The
    /// `query` can define the parameter placement by annotating them with
    /// `@PN`, where N is the index of the parameter, starting from `1`. If
    /// executing multiple queries at a time, delimit them with `;` and refer to
    /// [`ExecuteResult`] how to get results for the separate queries.
    ///
    /// For mapping of Rust types when writing, see the documentation for
    /// [`ToSql`]. For reading data from the database, see the documentation for
    /// [`FromSql`].
    ///
    /// This API is not quite suitable for dynamic query parameters. In these
    /// cases using a [`Query`] object might be easier.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tiberius::Config;
    /// # use std::env;
    /// #
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = std::net::TcpStream::connect(config.get_addr())?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp)?;
    /// let results = client
    ///     .execute(
    ///         "INSERT INTO ##Test (id) VALUES (@P1), (@P2), (@P3)",
    ///         &[&1i32, &2i32, &3i32],
    ///     )
    ///     ?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`ExecuteResult`]: struct.ExecuteResult.html
    /// [`ToSql`]: trait.ToSql.html
    /// [`FromSql`]: trait.FromSql.html
    /// [`Query`]: struct.Query.html
    pub(crate) fn execute<'a>(
        &mut self,
        query: impl Into<Cow<'a, str>>,
        params: &[&dyn ToSql],
    ) -> crate::mssql::connection::Result<ExecuteResult> {
        self.connection.flush_stream()?;
        let rpc_params = Self::rpc_params(query);

        let params = params.iter().map(|s| s.to_sql());
        self.rpc_perform_query(RpcProcId::ExecuteSQL, rpc_params, params)?;

        ExecuteResult::new(&mut self.connection)
    }

    /// Executes SQL statements in the SQL Server, returning resulting rows.
    /// Useful for `SELECT` statements. The `query` can define the parameter
    /// placement by annotating them with `@PN`, where N is the index of the
    /// parameter, starting from `1`. If executing multiple queries at a time,
    /// delimit them with `;` and refer to [`QueryStream`] on proper stream
    /// handling.
    ///
    /// For mapping of Rust types when writing, see the documentation for
    /// [`ToSql`]. For reading data from the database, see the documentation for
    /// [`FromSql`].
    ///
    /// This API can be cumbersome for dynamic query parameters. In these cases,
    /// if fighting too much with the compiler, using a [`Query`] object might be
    /// easier.
    ///
    /// # Example
    ///
    /// ```
    /// # use tiberius::Config;
    /// # use std::env;
    /// #
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = std::net::TcpStream::connect(config.get_addr())?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp)?;
    /// let stream = client
    ///     .query(
    ///         "SELECT @P1, @P2, @P3",
    ///         &[&1i32, &2i32, &3i32],
    ///     )
    ///     ?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`QueryStream`]: struct.QueryStream.html
    /// [`Query`]: struct.Query.html
    /// [`ToSql`]: trait.ToSql.html
    /// [`FromSql`]: trait.FromSql.html
    #[expect(dead_code)]
    pub(crate) fn query<'a, 'b>(
        &'a mut self,
        query: impl Into<Cow<'b, str>>,
        params: &'b [&'b dyn ToSql],
    ) -> crate::mssql::connection::Result<QueryStream<'a>>
    where
        'a: 'b,
    {
        self.connection.flush_stream()?;
        let rpc_params = Self::rpc_params(query);

        let params = params.iter().map(|p| p.to_sql());
        self.rpc_perform_query(RpcProcId::ExecuteSQL, rpc_params, params)?;

        let ts = TokenStream::new(&mut self.connection);
        let mut result = QueryStream::new(ts.try_unfold());
        result.forward_to_metadata()?;

        Ok(result)
    }

    /// Execute multiple queries, delimited with `;` and return multiple result
    /// sets; one for each query.
    ///
    /// # Example
    ///
    /// ```
    /// # use tiberius::Config;
    /// # use std::env;
    /// #
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = std::net::TcpStream::connect(config.get_addr())?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp)?;
    /// let row = client.simple_query("SELECT 1 AS col")?.into_row()?.unwrap();
    /// assert_eq!(Some(1i32), row.get("col"));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Warning
    ///
    /// Do not use this with any user specified input. Please resort to prepared
    /// statements using the [`query`] method.
    ///
    /// [`query`]: #method.query
    #[expect(dead_code)]
    pub(crate) fn simple_query<'a, 'b>(
        &'a mut self,
        query: impl Into<Cow<'b, str>>,
    ) -> crate::mssql::connection::Result<QueryStream<'a>>
    where
        'a: 'b,
    {
        self.connection.flush_stream()?;

        let req = BatchRequest::new(query, self.connection.context().transaction_descriptor());

        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::batch(id), req)?;

        let ts = TokenStream::new(&mut self.connection);

        let mut result = QueryStream::new(ts.try_unfold());
        result.forward_to_metadata()?;

        Ok(result)
    }

    /// Execute a `BULK INSERT` statement, efficiantly storing a large number of
    /// rows to a specified table. Note: make sure the input row follows the same
    /// schema as the table, otherwise calling `send()` will return an error.
    ///
    /// # Example
    ///
    /// ```
    /// # use tiberius::{Config, IntoRow};
    /// # use std::env;
    /// #
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = std::net::TcpStream::connect(config.get_addr())?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp)?;
    /// let create_table = r#"
    ///     CREATE TABLE ##bulk_test (
    ///         id INT IDENTITY PRIMARY KEY,
    ///         val INT NOT NULL
    ///     )
    /// "#;
    ///
    /// client.simple_query(create_table)?;
    ///
    /// // Start the bulk insert with the client.
    /// let mut req = client.bulk_insert("##bulk_test")?;
    ///
    /// for i in [0i32, 1i32, 2i32] {
    ///     let row = (i).into_row();
    ///
    ///     // The request will handle flushing to the wire in an optimal way,
    ///     // balancing between memory usage and IO performance.
    ///     req.send(row)?;
    /// }
    ///
    /// // The request must be finalized.
    /// let res = req.finalize()?;
    /// assert_eq!(3, res.total());
    /// # Ok(())
    /// # }
    /// ```
    #[expect(dead_code)]
    pub(crate) fn bulk_insert<'a>(
        &'a mut self,
        table: &'a str,
    ) -> crate::mssql::connection::Result<BulkLoadRequest<'a, S>> {
        // Start the bulk request
        self.connection.flush_stream()?;

        // retrieve column metadata from server
        let query = format!("SELECT TOP 0 * FROM {}", table);

        let req = BatchRequest::new(query, self.connection.context().transaction_descriptor());

        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::batch(id), req)?;

        let mut token_stream = TokenStream::new(&mut self.connection).try_unfold();

        let columns = token_stream.try_fold(None, move |mut columns, token| match token {
            Ok(token) => {
                if let ReceivedToken::NewResultset(metadata) = token {
                    columns = Some(metadata.columns.clone());
                };
                Ok(columns)
            }
            Err(e) => Err(e),
        })?;

        drop(token_stream);

        // now start bulk upload
        let columns: Vec<_> = columns
            .ok_or_else(|| {
                crate::mssql::connection::Error::Protocol(
                    "expecting column metadata from query but not found".into(),
                )
            })?
            .into_iter()
            .filter(|column| column.base.flags.contains(ColumnFlag::Updateable))
            .collect();

        self.connection.flush_stream()?;
        let col_data = columns.iter().map(|c| format!("{}", c)).join(", ");
        let query = format!("INSERT BULK {} ({})", table, col_data);

        let req = BatchRequest::new(query, self.connection.context().transaction_descriptor());
        let id = self.connection.context_mut().next_packet_id();

        self.connection.send(PacketHeader::batch(id), req)?;

        let ts = TokenStream::new(&mut self.connection);
        ts.flush_done()?;

        BulkLoadRequest::new(&mut self.connection, columns)
    }

    /// Closes this database connection explicitly.
    #[expect(dead_code)]
    pub(crate) fn close(self) -> crate::mssql::connection::Result<()> {
        Ok(())
    }

    pub(crate) fn rpc_params<'a>(query: impl Into<Cow<'a, str>>) -> Vec<RpcParam<'a>> {
        let query = query.into();
        let query = query.replace("\n", "\n\r").replace("\r\r", "\r");
        vec![
            RpcParam {
                name: Cow::Borrowed("stmt"),
                flags: BitFlags::empty(),
                value: ColumnData::String(Some(query.into())),
            },
            RpcParam {
                name: Cow::Borrowed("params"),
                flags: BitFlags::empty(),
                value: ColumnData::I32(Some(0)),
            },
        ]
    }

    pub(crate) fn rpc_perform_query<'a, 'b>(
        &'a mut self,
        proc_id: RpcProcId,
        mut rpc_params: Vec<RpcParam<'b>>,
        params: impl Iterator<Item = ColumnData<'b>>,
    ) -> crate::mssql::connection::Result<()>
    where
        'a: 'b,
    {
        let mut param_str = String::new();

        for (i, param) in params.enumerate() {
            if i > 0 {
                param_str.push(',')
            }
            param_str.push_str(&format!("@P{} ", i + 1));
            param_str.push_str(&param.type_name());

            rpc_params.push(RpcParam {
                name: Cow::Owned(format!("@P{}", i + 1)),
                flags: BitFlags::empty(),
                value: param,
            });
        }

        if let Some(params) = rpc_params.iter_mut().find(|x| x.name == "params") {
            params.value = ColumnData::String(Some(param_str.into()));
        }

        let req = TokenRpcRequest::new(
            proc_id,
            rpc_params,
            self.connection.context().transaction_descriptor(),
        );

        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::rpc(id), req)?;

        Ok(())
    }
}
