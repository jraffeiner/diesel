//! The Firebird connection

use super::backend::Fb;
use super::query_builder::FbQueryBuilder;
use super::transaction::FbTransactionManager;
use super::value::FbRow;
use crate::connection::*;
use crate::expression::QueryMetadata;
use crate::query_builder::bind_collector::RawBytesBindCollector;
use crate::query_builder::*;

#[cfg(feature = "r2d2")]
use crate::r2d2::R2D2Connection;
use crate::result::Error::DatabaseError;
use crate::result::*;
use rsfbclient::{Execute, SqlType};
use rsfbclient::{Queryable, Row, SimpleConnection as FbRawConnection};

#[allow(missing_docs, missing_debug_implementations)]
pub struct FbConnection {
    pub raw: FbRawConnection,
    tr_manager: FbTransactionManager,
    instrumentation: Option<Box<dyn Instrumentation>>,
}

impl SimpleConnection for FbConnection {
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.raw
            .execute(query, ())
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))
            .map(|_| ())
    }
}

impl ConnectionSealed for FbConnection {}

impl Connection for FbConnection {
    type TransactionManager = FbTransactionManager;
    type Backend = Fb;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let mut raw_builder = rsfbclient::builder_pure_rust();

        let raw = raw_builder
            .from_string(database_url)
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?
            .connect()
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        FbConnection::init(raw.into())
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        let mut bc = RawBytesBindCollector::<Fb>::new();
        source.collect_binds(&mut bc, &mut (), &Fb)?;

        let mut qb = FbQueryBuilder::new();
        source.to_sql(&mut qb, &Fb)?;
        let sql = qb.finish();

        let params: Vec<SqlType> = bc
            .metadata
            .into_iter()
            .zip(bc.binds)
            .map(|(tp, val)| tp.into_param(val))
            .collect();

        self.raw
            .execute(&sql, params)
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData {
        &mut self.tr_manager
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        &mut self.instrumentation
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = Some(Box::new(instrumentation));
    }

    fn set_prepared_statement_cache_size(&mut self, _size: CacheSize) {}
}

#[expect(dead_code)]
trait Helper {
    fn load<'conn, 'query, T>(
        conn: &'conn mut FbConnection,
        source: T,
    ) -> QueryResult<Box<dyn Iterator<Item = QueryResult<FbRow>>>>
    where
        T: Query + QueryFragment<Fb> + QueryId + 'query,
        Fb: diesel::expression::QueryMetadata<T::SqlType>;
}

impl LoadConnection<DefaultLoadingMode> for FbConnection {
    type Cursor<'conn, 'query>
        = FbCursor
    where
        Self: 'conn;

    type Row<'conn, 'query>
        = FbRow
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
        let source = &source.as_query();
        let mut bc = RawBytesBindCollector::<Fb>::new();
        source.collect_binds(&mut bc, &mut (), &Fb)?;

        let mut qb = FbQueryBuilder::new();
        source.to_sql(&mut qb, &Fb)?;
        let has_cursor = qb.has_cursor;
        let sql = qb.finish();

        let params: Vec<SqlType> = bc
            .metadata
            .into_iter()
            .zip(bc.binds)
            .map(|(tp, val)| tp.into_param(val))
            .collect();

        let results = if has_cursor {
            self.raw.query::<Vec<SqlType>, Row>(&sql, params)
        } else {
            match self
                .raw
                .execute_returnable::<Vec<SqlType>, Row>(&sql, params)
            {
                Ok(result) => Ok(vec![result]),
                Err(e) => Err(e),
            }
        };

        Ok(results
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?
            .into())
    }
}

#[cfg(feature = "r2d2")]
impl R2D2Connection for FbConnection {
    fn ping(&mut self) -> QueryResult<()> {
        self.batch_execute("SELECT 1 FROM RDB$DATABASE")
    }
}

impl FbConnection {
    /// Create a diesel instance from a active firebird connection
    pub fn init(conn: FbRawConnection) -> ConnectionResult<Self> {
        Ok(FbConnection {
            raw: conn,
            tr_manager: FbTransactionManager::new(),
            instrumentation: None,
        })
    }
}

#[allow(missing_docs, missing_debug_implementations)]
pub struct FbCursor {
    results: Vec<FbRow>,
}

impl Iterator for FbCursor {
    type Item = QueryResult<FbRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.results.pop().map(Ok)
    }
}

impl From<Vec<Row>> for FbCursor {
    fn from(value: Vec<Row>) -> Self {
        FbCursor {
            results: value.into_iter().map(FbRow::new).collect(),
        }
    }
}
