use super::backend::Mssql;
use super::connection::ColumnData;
use super::MssqlType;
use crate::query_builder::{BindCollector, MoveableBindCollector, QueryBuilder};
use crate::result::QueryResult;
use crate::serialize::Output;
use crate::sql_types::{self, HasSqlType};

mod query_fragment_impls;

/// The Mssql query builder
#[allow(missing_debug_implementations)]
#[derive(Default)]
pub struct MssqlQueryBuilder {
    sql: String,
    param_num: u32,
}

impl MssqlQueryBuilder {
    /// Constructs a new query builder with an empty query
    pub fn new() -> Self {
        MssqlQueryBuilder::default()
    }
}

impl QueryBuilder<Mssql> for MssqlQueryBuilder {
    fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    fn push_identifier(&mut self, identifier: &str) -> QueryResult<()> {
        self.push_sql(identifier);
        Ok(())
    }

    fn push_bind_param(&mut self) {
        self.param_num += 1;
        self.sql.push_str(&format!("@P{}", self.param_num));
    }

    fn finish(self) -> String {
        self.sql
    }
}

#[derive(Debug, Default, Clone)]
pub struct TdsBindCollector<'a> {
    pub(crate) data: Vec<ColumnData<'a>>,
    pub(crate) types: Vec<MssqlType>,
}

impl<'a> BindCollector<'a, Mssql> for TdsBindCollector<'a> {
    type Buffer = Option<ColumnData<'a>>;

    fn push_bound_value<T, U>(
        &mut self,
        bind: &'a U,
        metadata_lookup: &mut <Mssql as diesel::sql_types::TypeMetadata>::MetadataLookup,
    ) -> diesel::QueryResult<()>
    where
        Mssql: diesel::backend::Backend + diesel::sql_types::HasSqlType<T>,
        U: diesel::serialize::ToSql<T, Mssql> + ?Sized + 'a,
    {
        let cd = None;
        let mut output = Output::<Mssql>::new(cd, metadata_lookup);
        if let Err(e) = bind.to_sql(&mut output) {
            Err(diesel::result::Error::QueryBuilderError(e))
        } else {
            self.data
                .push(output.into_inner().unwrap_or(ColumnData::U8(None)));
            self.types
                .push(<Mssql as HasSqlType<T>>::metadata(metadata_lookup));
            Ok(())
        }
    }
}

impl MoveableBindCollector<Mssql> for TdsBindCollector<'static> {
    #[doc = " The movable bind data of this bind collector"]
    type BindData = Self;

    #[doc = " Builds a movable version of the bind collector"]
    fn moveable(&self) -> Self::BindData {
        self.clone()
    }

    #[doc = " Refill the bind collector with its bind data"]
    fn append_bind_data(&mut self, from: &Self::BindData) {
        self.data.append(&mut from.data.clone());
    }
}

impl TdsBindCollector<'_> {
    pub fn new() -> Self {
        Self::default()
    }
}

macro_rules! to_diesel_sql {
    ($sql_type:ty, $target:ty) => {
        impl diesel::serialize::ToSql<$sql_type, Mssql> for $target {
            fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Mssql>) -> diesel::serialize::Result {
                out.set_value(crate::mssql::connection::ToSql::to_sql(self));
                Ok(diesel::serialize::IsNull::No)
            }
        }
    };
}

to_diesel_sql!(sql_types::TinyInt, u8);
to_diesel_sql!(sql_types::Binary, [u8]);
to_diesel_sql!(sql_types::BigInt, i64);
to_diesel_sql!(sql_types::Bool, bool);
to_diesel_sql!(sql_types::Double, f64);
to_diesel_sql!(sql_types::Float, f32);
to_diesel_sql!(sql_types::Integer, i32);
to_diesel_sql!(sql_types::SmallInt, i16);
to_diesel_sql!(sql_types::Text, str);

#[cfg(feature = "rust_decimal")]
to_diesel_sql!(sql_types::Decimal, Decimal);

#[cfg(feature = "bigdecimal")]
to_diesel_sql!(sql_types::Decimal, bigdecimal::BigDecimal);

#[cfg(feature = "chrono")]
mod chrono_impl {
    use super::Mssql;
    use super::Output;
    use chrono::DateTime;
    use chrono::TimeZone;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use diesel::sql_types;

    to_diesel_sql!(sql_types::Timestamp, NaiveDateTime);
    to_diesel_sql!(sql_types::Time, NaiveTime);
    to_diesel_sql!(sql_types::Date, NaiveDate);

    impl<Tz: TimeZone> diesel::serialize::ToSql<sql_types::DateTimeOffset, Mssql> for DateTime<Tz>
    where
        <Tz as TimeZone>::Offset: Send + Sync,
    {
        fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Mssql>) -> crate::serialize::Result {
            out.set_value(crate::mssql::connection::ToSql::to_sql(self));
            Ok(diesel::serialize::IsNull::No)
        }
    }
}

#[cfg(feature = "time")]
mod time_impl {
    use super::Mssql;
    use super::Output;
    use diesel::sql_types;
    use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

    to_diesel_sql!(sql_types::Timestamp, PrimitiveDateTime);
    to_diesel_sql!(sql_types::Time, Time);
    to_diesel_sql!(sql_types::Date, Date);
    to_diesel_sql!(sql_types::Timestamp, OffsetDateTime);
    to_diesel_sql!(super::sql_types::DateTimeOffset, OffsetDateTime);
}
