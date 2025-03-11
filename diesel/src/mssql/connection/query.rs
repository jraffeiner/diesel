use std::borrow::Cow;

use std::io::{Read, Write};

use crate::mssql::connection::{
    tds::{codec::RpcProcId, stream::TokenStream},
    ColumnData, ExecuteResult, QueryStream,
};

use super::client::Client;
use super::to_sql::IntoSql;

/// A query object with bind parameters.
#[derive(Debug)]
pub(crate) struct Query<'a> {
    sql: Cow<'a, str>,
    params: Vec<ColumnData<'a>>,
}

impl<'a> Query<'a> {
    /// Construct a new query object with the given SQL. If the SQL is
    /// parameterized, the given number of parameters must be bound to the
    /// object before executing.
    ///
    /// The `sql` can define the parameter placement by annotating them with
    /// `@PN`, where N is the index of the parameter, starting from `1`.
    #[expect(unused)]
    pub(crate) fn new(sql: impl Into<Cow<'a, str>>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    /// Bind a new parameter to the query. Must be called exactly as many times
    /// as there are parameters in the given SQL. Otherwise the query will fail
    /// on execution.
    #[expect(unused)]
    pub(crate) fn bind(&mut self, param: impl IntoSql<'a> + 'a) {
        self.params.push(param.into_sql());
    }

    /// Executes SQL statements in the SQL Server, returning the number rows
    /// affected. Useful for `INSERT`, `UPDATE` and `DELETE` statements. See
    /// [`Client#execute`] for a simpler API if the parameters are statically
    /// known.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tiberius::{Config, Query};
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
    /// let mut query = Query::new("INSERT INTO ##Test (id) VALUES (@P1), (@P2), (@P3)");
    ///
    /// query.bind("foo");
    /// query.bind(2i32);
    /// query.bind(String::from("bar"));
    ///
    /// let results = query.execute(&mut client)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`ToSql`]: trait.ToSql.html
    /// [`FromSql`]: trait.FromSql.html
    /// [`Client#execute`]: struct.Client.html#method.execute
    #[expect(unused)]
    pub(crate) fn execute<S>(
        self,
        client: &mut Client<S>,
    ) -> crate::mssql::connection::Result<ExecuteResult>
    where
        S: Read + Write + Send,
    {
        client.connection.flush_stream()?;

        let rpc_params = Client::<S>::rpc_params(self.sql);

        client.rpc_perform_query(RpcProcId::ExecuteSQL, rpc_params, self.params.into_iter())?;

        ExecuteResult::new(&mut client.connection)
    }

    /// Executes SQL statements in the SQL Server, returning resulting rows.
    /// Useful for `SELECT` statements. See [`Client#query`] for a simpler API
    /// if the parameters are statically known.
    ///
    /// # Example
    ///
    /// ```
    /// # use tiberius::{Config, Query};
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
    /// let mut query = Query::new("SELECT @P1, @P2, @P3");
    ///
    /// query.bind(1i32);
    /// query.bind(2i32);
    /// query.bind(3i32);
    ///
    /// let stream = query.query(&mut client)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`QueryStream`]: struct.QueryStream.html
    /// [`ToSql`]: trait.ToSql.html
    /// [`FromSql`]: trait.FromSql.html
    /// [`Client#query`]: struct.Client.html#method.query
    #[expect(unused)]
    pub(crate) fn query<S>(
        self,
        client: &mut Client<S>,
    ) -> crate::mssql::connection::Result<QueryStream<'_>>
    where
        S: Read + Write + Send,
    {
        client.connection.flush_stream()?;
        let rpc_params = Client::<S>::rpc_params(self.sql);

        client.rpc_perform_query(RpcProcId::ExecuteSQL, rpc_params, self.params.into_iter())?;

        let ts = TokenStream::new(&mut client.connection);
        let mut result = QueryStream::new(ts.try_unfold());
        result.forward_to_metadata()?;

        Ok(result)
    }
}
