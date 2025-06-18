//! An asynchronous, runtime-independent, pure-rust Tabular Data Stream (TDS)
//! implementation for Microsoft SQL Server.
//!
//! # Connecting with async-std
//!
//! Being not bound to any single runtime, a `TcpStream` must be created
//! separately and injected to the [`Client`].
//!
//! ```no_run
//! use tiberius::{Client, Config, Query, AuthMethod};
//!
//! fn main() -> anyhow::Result<()> {
//!     // Using the builder method to construct the options.
//!     let mut config = Config::new();
//!
//!     config.host("localhost");
//!     config.port(1433);
//!
//!     // Using SQL Server authentication.
//!     config.authentication(AuthMethod::sql_server("SA", "<YourStrong@Passw0rd>"));
//!
//!     // on production, it is not a good idea to do this
//!     config.trust_cert();
//!
//!     // Taking the address from the configuration, using async-std's
//!     // TcpStream to connect to the server.
//!     let tcp = std::net::TcpStream::connect(config.get_addr())?;
//!
//!     // We'll disable the Nagle algorithm. Buffering is handled
//!     // internally with a `Sink`.
//!     tcp.set_nodelay(true)?;
//!
//!     // Handling TLS, login and other details related to the SQL Server.
//!     let mut client = Client::connect(config, tcp)?;
//!
//!     // Constructing a query object with one parameter annotated with `@P1`.
//!     // This requires us to bind a parameter that will then be used in
//!     // the statement.
//!     let mut select = Query::new("SELECT @P1");
//!     select.bind(-4i32);
//!
//!     // A response to a query is a stream of data, that must be
//!     // polled to the end before querying again. Using streams allows
//!     // fetching data in an asynchronous manner, if needed.
//!     let stream = select.query(&mut client)?;
//!
//!     // In this case, we know we have only one query, returning one row
//!     // and one column, so calling `into_row` will consume the stream
//!     // and return us the first row of the first result.
//!     let row = stream.into_row()?;
//!
//!     assert_eq!(Some(-4i32), row.unwrap().get(0));
//!
//!     Ok(())
//! }
//! ```
//!
//! # Connecting with Tokio
//!
//! Tokio is using their own version of `Read` and `Write` traits,
//! meaning that when wanting to use Tiberius with Tokio, their `TcpStream`
//! needs to be wrapped in Tokio's `Compat` module.
//!
//! ```no_run
//! use tiberius::{Client, Config, AuthMethod};
//!
//! fn main() -> anyhow::Result<()> {
//!     let mut config = Config::new();
//!
//!     config.host("localhost");
//!     config.port(1433);
//!     config.authentication(AuthMethod::sql_server("SA", "<YourStrong@Passw0rd>"));
//!     config.trust_cert(); // on production, it is not a good idea to do this
//!
//!     let tcp = std::net::TcpStream::connect(config.get_addr())?;
//!     tcp.set_nodelay(true)?;
//!
//!     // To be able to use Tokio's tcp, we're using the `compat_write` from
//!     // the `TokioWriteCompatExt` to get a stream compatible with the
//!     // traits from the `futures` crate.
//!     let mut client = Client::connect(config, tcp)?;
//!     # client.query("SELECT @P1", &[&-4i32])?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Ways of querying
//!
//! Tiberius offers two ways to query the database: directly from the [`Client`]
//! with the [`Client#query`] and [`Client#execute`], or additionally through
//! the [`Query`] object.
//!
//! ### With the client methods
//!
//! When the query parameters are known when writing the code, the client methods
//! are easy to use.
//!
//! ```no_run
//! # use tiberius::{Client, Config, AuthMethod};
//! # fn main() -> anyhow::Result<()> {
//! # let mut config = Config::new();
//! # config.host("localhost");
//! # config.port(1433);
//! # config.authentication(AuthMethod::sql_server("SA", "<YourStrong@Passw0rd>"));
//! # config.trust_cert();
//! # let tcp = std::net::TcpStream::connect(config.get_addr())?;
//! # tcp.set_nodelay(true)?;
//! # let mut client = Client::connect(config, tcp)?;
//! let _res = client.query("SELECT @P1", &[&-4i32])?;
//! # Ok(())
//! # }
//! ```
//!
//! ### With the Query object
//!
//! In case of needing to pass the parameters from a dynamic collection, or if
//! wanting to pass them by-value, use the [`Query`] object.
//!
//! ```no_run
//! # use tiberius::{Client, Query, Config, AuthMethod};
//! # fn main() -> anyhow::Result<()> {
//! # let mut config = Config::new();
//! # config.host("localhost");
//! # config.port(1433);
//! # config.authentication(AuthMethod::sql_server("SA", "<YourStrong@Passw0rd>"));
//! # config.trust_cert();
//! # let tcp = std::net::TcpStream::connect(config.get_addr())?;
//! # tcp.set_nodelay(true)?;
//! # let mut client = Client::connect(config, tcp)?;
//! let params = vec![String::from("foo"), String::from("bar")];
//! let mut select = Query::new("SELECT @P1, @P2, @P3");
//!
//! for param in params.into_iter() {
//!     select.bind(param);
//! }
//!
//! let _res = select.query(&mut client)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Authentication
//!
//! Tiberius supports different [ways of authentication] to the SQL Server:
//!
//! - SQL Server authentication uses the facilities of the database to
//!   authenticate the user.
//! - On Windows, you can authenticate using the currently logged in user or
//!   specified Windows credentials.
//! - If enabling the `integrated-auth-gssapi` feature, it is possible to login
//!   with the currently active Kerberos credentials.
//!
//! ## AAD(Azure Active Directory) Authentication
//!
//! Tiberius supports AAD authentication by taking an AAD token. Suggest using
//! [azure_identity](https://crates.io/crates/azure_identity) crate to retrieve
//! the token, and config tiberius with token. There is an example in examples
//! folder on how to setup this.
//!
//! # TLS
//!
//! When compiled using the default features, a TLS encryption will be available
//! and by default, used for all traffic. TLS is handled with the given
//! `TcpStream`. Please see the documentation for [`EncryptionLevel`] for
//! details.
//!
//! # SQL Browser
//!
//! On Windows platforms, connecting to the SQL Server might require going through
//! the SQL Browser service to get the correct port for the named instance. This
//! feature requires either the `sql-browser-async-std` or `sql-browser-tokio` feature
//! flag to be enabled and has a bit different way of connecting:
//!
//! ```no_run
//! # #[cfg(feature = "sql-browser")]
//! use tiberius::{Client, Config, AuthMethod};
//!
//! // An extra trait that allows connecting to a named instance with the given
//! // `TcpStream`.
//! # #[cfg(feature = "sql-browser")]
//! use tiberius::SqlBrowser;
//!
//! # #[cfg(feature = "sql-browser")]
//! fn main() -> anyhow::Result<()> {
//!     let mut config = Config::new();
//!
//!     config.authentication(AuthMethod::sql_server("SA", "<password>"));
//!     config.host("localhost");
//!
//!     // The default port of SQL Browser
//!     config.port(1434);
//!
//!     // The name of the database server instance.
//!     config.instance_name("INSTANCE");
//!
//!     // on production, it is not a good idea to do this
//!     config.trust_cert();
//!
//!     // This will create a new `TcpStream` from `async-std`, connected to the
//!     // right port of the named instance.
//!     let tcp = std::net::TcpStream::connect_named(&config)?;
//!
//!     // And from here on continue the connection process in a normal way.
//!     let mut client = Client::connect(config, tcp)?;
//!     # client.query("SELECT @P1", &[&-4i32])?;
//!     Ok(())
//! }
//! # #[cfg(not(feature = "sql-browser"))]
//! # fn main() {}
//! ```
//!
//! # Other features
//!
//! - If using an [ADO.NET connection string], it is possible to create a
//!   [`Config`] from one. Please see the documentation for
//!   [`from_ado_string`] for details.
//! - If wanting to use Tiberius with SQL Server version 2005, one must
//!   disable the `tds73` feature.
//!
//! [`EncryptionLevel`]: enum.EncryptionLevel.html
//! [`Client`]: struct.Client.html
//! [`Client#query`]: struct.Client.html#method.query
//! [`Client#execute`]: struct.Client.html#method.execute
//! [`Query`]: struct.Query.html
//! [`Query#bind`]: struct.Query.html#method.bind
//! [`Config`]: struct.Config.html
//! [`from_ado_string`]: struct.Config.html#method.from_ado_string
//! [`time`]: time/index.html
//! [ways of authentication]: enum.AuthMethod.html
//! [ADO.NET connection string]: https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings

#[macro_use]
mod macros;

mod client;
mod from_sql;
mod query;
mod sql_read_bytes;
mod to_sql;

pub(crate) mod error;
mod result;
mod row;
mod tds;

mod transaction_manager;

use client::{AuthMethod, Client, Config};
pub(crate) use error::Error;
pub(crate) use from_sql::{FromSql, FromSqlOwned};
pub(crate) use result::*;
use row::MssqlCursor;
pub(crate) use row::{Column, ColumnType, MssqlRow};
pub(crate) use tds::codec::framed;
pub(crate) use tds::{
    codec::{BulkLoadRequest, ColumnData, ColumnFlag, TokenRow},
    stream::QueryStream,
    time, xml, EncryptionLevel,
};
pub(crate) use to_sql::ToSql;

use sql_read_bytes::*;
use tds::codec::*;
use tracing::{error, info, trace};
use transaction_manager::MssqlTransactionManager;

/// An alias for a result that holds crate's error type as the error.
pub(crate) type Result<T> = std::result::Result<T, Error>;

pub(crate) fn get_driver_version() -> u64 {
    env!("CARGO_PKG_VERSION")
        .splitn(6, '.')
        .enumerate()
        .fold(0u64, |acc, part| match part.1.parse::<u64>() {
            Ok(num) => acc | num << (part.0 * 8),
            _ => acc | 0 << (part.0 * 8),
        })
}

use std::net::TcpStream;

use crate::connection::{TransactionManagerStatus, ValidTransactionManagerStatus};
#[cfg(feature = "r2d2")]
use crate::r2d2::R2D2Connection;

use crate::{
    connection::{
        self, instrumentation::StrQueryHelper, ConnectionSealed, DefaultLoadingMode,
        Instrumentation, InstrumentationEvent, LoadConnection, MultiConnectionHelper,
        SimpleConnection, TransactionManager,
    },
    debug_query,
    expression::QueryMetadata,
    migration::MigrationConnection,
    mssql::{
        connection::tds::stream::TokenStream, query_builder::TdsBindCollector, MssqlQueryBuilder,
    },
    query_builder::{Query, QueryBuilder, QueryFragment, QueryId},
    Connection, ConnectionError, ConnectionResult, QueryResult, RunQueryDsl,
};
use url::Url;

use super::Mssql;

type TibClient = Client<TcpStream>;

/// Connection Struct for Mssql Connections
#[expect(clippy::module_name_repetitions)]
pub struct MssqlConnection {
    client: TibClient,
    transaction_manager: MssqlTransactionManager,
    instrumentation: Option<Box<dyn Instrumentation>>,
}

impl std::fmt::Debug for MssqlConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlConnection")
            .field("client", &self.client)
            .field("transaction_manager", &self.transaction_manager)
            .finish_non_exhaustive()
    }
}

impl ConnectionSealed for MssqlConnection {}

impl SimpleConnection for MssqlConnection {
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.instrumentation
            .on_connection_event(InstrumentationEvent::StartQuery {
                query: &StrQueryHelper::new(query),
            });
        info!("Batch Execute: {query}");
        let r = self.client.execute(query, &[]);
        let r = match r {
            Ok(_) => Ok(()),
            Err(diesel::mssql::connection::error::Error::Server(TokenError {
                code: 3903,
                state: 1,
                class: 16,
                ..
            })) => {
                self.transaction_manager.status =
                    TransactionManagerStatus::Valid(ValidTransactionManagerStatus {
                        in_transaction: None,
                    });
                Err(diesel::result::Error::NotInTransaction)
            }
            Err(e) => Err(diesel::result::Error::DeserializationError(Box::new(e))),
        };
        self.instrumentation
            .on_connection_event(InstrumentationEvent::FinishQuery {
                query: &StrQueryHelper::new(query),
                error: r.as_ref().err(),
            });
        r
    }
}

impl MssqlConnection {
    /// Creates a `MssqlConnection` from a `Config`
    pub fn from_config(config: Config) -> ConnectionResult<Self> {
        let transaction_manager = MssqlTransactionManager::default();
        let mut instrumentation = connection::get_default_instrumentation();
        instrumentation.on_connection_event(InstrumentationEvent::StartEstablishConnection {
            url: "From Config",
        });
        let tcp_stream = match TcpStream::connect(config.get_addr()) {
            Ok(stream) => stream,
            Err(e) => return Err(ConnectionError::BadConnection(e.to_string())),
        };
        match tcp_stream.set_nodelay(true) {
            Ok(()) => (),
            Err(e) => return Err(ConnectionError::BadConnection(e.to_string())),
        };
        let conn_res = TibClient::connect(config, tcp_stream)
            .map_err(|e| ConnectionError::InvalidConnectionUrl(e.to_string()));
        instrumentation.on_connection_event(InstrumentationEvent::FinishEstablishConnection {
            url: "From Config",
            error: conn_res.as_ref().err(),
        });
        let client = conn_res?;
        Ok(Self {
            client,
            transaction_manager,
            instrumentation,
        })
    }
}

impl Connection for MssqlConnection {
    type Backend = Mssql;

    type TransactionManager = MssqlTransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let transaction_manager = MssqlTransactionManager::default();
        let mut instrumentation = connection::get_default_instrumentation();
        instrumentation.on_connection_event(InstrumentationEvent::StartEstablishConnection {
            url: database_url,
        });

        let url = Url::parse(database_url)
            .map_err(|e| ConnectionError::InvalidConnectionUrl(e.to_string()))?;

        let user = url.username();
        let password =
            percent_encoding::percent_decode(url.password().unwrap_or_default().as_bytes())
                .decode_utf8_lossy();
        let host = url.host_str().unwrap_or_default();
        let port = url.port().unwrap_or(1433);
        let database = url
            .path_segments()
            .and_then(|mut s| s.next())
            .unwrap_or("master");

        let mut config = crate::mssql::connection::Config::new();
        config.authentication(AuthMethod::sql_server(user, password));
        config.host(host);
        config.port(port);
        config.database(database);
        config.trust_cert();

        let tcp_stream = match TcpStream::connect(config.get_addr()) {
            Ok(stream) => stream,
            Err(e) => return Err(ConnectionError::BadConnection(e.to_string())),
        };
        match tcp_stream.set_nodelay(true) {
            Ok(()) => (),
            Err(e) => return Err(ConnectionError::BadConnection(e.to_string())),
        };
        let conn_res = TibClient::connect(config, tcp_stream)
            .map_err(|e| ConnectionError::InvalidConnectionUrl(e.to_string()));
        instrumentation.on_connection_event(InstrumentationEvent::FinishEstablishConnection {
            url: database_url,
            error: conn_res.as_ref().err(),
        });
        let client = conn_res?;
        Ok(Self {
            client,
            transaction_manager,
            instrumentation,
        })
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        let (query, params) = prepare_query(source)?;
        let res = get_results(&mut self.client, &query, params);
        match res {
            Ok(res) => Ok(res.total().try_into().unwrap_or(usize::MAX)),
            Err(e) => Err(diesel::result::Error::SerializationError(Box::new(e))),
        }
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData {
        &mut self.transaction_manager
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        &mut self.instrumentation
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = Some(Box::new(instrumentation));
    }

    fn set_prepared_statement_cache_size(&mut self, _size: connection::CacheSize) {
        error!("SetPreparedStatementCacheSize not implemented for mssql connection");
    }
}

fn prepare_query(source: &impl QueryFragment<Mssql>) -> QueryResult<(String, Vec<ColumnData<'_>>)> {
    let mut tqb = MssqlQueryBuilder::new();
    source.to_sql(&mut tqb, &Mssql)?;
    let query = tqb.finish();
    trace!("Preparing Query: {query}");
    let mut bind_collector = TdsBindCollector::default();
    source.collect_binds(&mut bind_collector, &mut (), &Mssql)?;
    let params = bind_collector.data;
    trace!("With Parameter: {params:?}");
    Ok((query, params))
}

fn get_query_stream<'a>(
    client: &'a mut TibClient,
    query: &str,
    params: Vec<ColumnData<'_>>,
) -> crate::mssql::connection::Result<QueryStream<'a>> {
    client.connection.flush_stream()?;
    let rpc_params = TibClient::rpc_params(query);

    client.rpc_perform_query(RpcProcId::ExecuteSQL, rpc_params, params.into_iter())?;

    let ts = TokenStream::new(&mut client.connection);
    let mut result = QueryStream::new(ts.try_unfold());
    result.forward_to_metadata()?;
    Ok(result)
}

fn get_results(
    client: &mut TibClient,
    query: &str,
    params: Vec<ColumnData<'_>>,
) -> crate::mssql::connection::Result<ExecuteResult> {
    client.connection.flush_stream()?;
    let rpc_params = TibClient::rpc_params(query);

    client.rpc_perform_query(RpcProcId::ExecuteSQL, rpc_params, params.into_iter())?;

    ExecuteResult::new(&mut client.connection)
}

impl LoadConnection<DefaultLoadingMode> for MssqlConnection {
    type Cursor<'conn, 'query>
        = MssqlCursor
    where
        Self: 'conn;

    type Row<'conn, 'query>
        = MssqlRow
    where
        Self: 'conn;

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<Self::Cursor<'conn, 'query>>
    where
        T: Query + QueryFragment<Self::Backend> + QueryId + 'query,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        let debug_query = debug_query(&source);
        let (query, params) = prepare_query(&source)?;
        self.instrumentation
            .on_connection_event(InstrumentationEvent::StartQuery {
                query: &debug_query,
            });
        let result = get_query_stream(&mut self.client, &query, params)
            .map_err(|e| diesel::result::Error::SerializationError(Box::new(e)));
        self.instrumentation
            .on_connection_event(InstrumentationEvent::FinishQuery {
                query: &debug_query,
                error: result.as_ref().err(),
            });
        let stream = result?;
        MssqlCursor::gather_stream(stream)
    }
}

#[cfg(feature = "r2d2")]
impl R2D2Connection for MssqlConnection {
    fn ping(&mut self) -> QueryResult<()> {
        self.batch_execute("SELECT 1")
    }
}

impl MultiConnectionHelper for MssqlConnection {
    fn to_any<'a>(
        lookup: &mut <<MssqlConnection as connection::Connection>::Backend as diesel::sql_types::TypeMetadata>::MetadataLookup,
    ) -> &mut (dyn std::any::Any + 'a) {
        lookup
    }

    fn from_any(
        lookup: &mut dyn std::any::Any,
    ) -> Option<&mut <Self::Backend as diesel::sql_types::TypeMetadata>::MetadataLookup> {
        lookup.downcast_mut()
    }
}

const CREATE_MIGRATIONS_TABLE: &str = "
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='__diesel_schema_migrations') 
CREATE TABLE __diesel_schema_migrations (
       version VARCHAR(50) PRIMARY KEY NOT NULL,
       run_on DATETIMEOFFSET NOT NULL DEFAULT GETUTCDATE()
);";

impl MigrationConnection for MssqlConnection {
    fn setup(&mut self) -> QueryResult<usize> {
        diesel::sql_query(CREATE_MIGRATIONS_TABLE).execute(self)
    }
}
