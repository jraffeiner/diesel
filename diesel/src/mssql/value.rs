use crate::sql_types::{BigInt, Binary, Bool, Double, Float, Integer, SmallInt, Text, TinyInt};

macro_rules! from_diesel_sql {
    ($sql_type:ty, $target:ty) => {
        impl diesel::deserialize::FromSql<$sql_type, crate::mssql::Mssql> for $target {
            fn from_sql(
                bytes: <crate::mssql::Mssql as diesel::backend::Backend>::RawValue<'_>,
            ) -> diesel::deserialize::Result<Self> {
                let res = crate::mssql::connection::FromSql::from_sql(&bytes)?;
                res.ok_or(unexpected_null())
            }
        }
    };
}

macro_rules! owned_from_diesel_sql {
    ($sql_type:ty, $target:ty) => {
        impl diesel::deserialize::FromSql<$sql_type, crate::mssql::Mssql> for $target {
            fn from_sql(
                bytes: <crate::mssql::Mssql as diesel::backend::Backend>::RawValue<'_>,
            ) -> diesel::deserialize::Result<Self> {
                let res = crate::mssql::connection::FromSqlOwned::from_sql_owned(bytes)?;
                res.ok_or(unexpected_null())
            }
        }
    };
}

owned_from_diesel_sql!(Text, String);
owned_from_diesel_sql!(Binary, Vec<u8>);
from_diesel_sql!(BigInt, i64);
from_diesel_sql!(Bool, bool);
from_diesel_sql!(SmallInt, i16);
from_diesel_sql!(Integer, i32);
from_diesel_sql!(Integer, u64);
from_diesel_sql!(Double, f64);
from_diesel_sql!(Float, f32);
from_diesel_sql!(TinyInt, u8);

fn unexpected_null() -> Box<diesel::result::UnexpectedNullError> {
    Box::new(diesel::result::UnexpectedNullError)
}

#[cfg(feature = "rust_decimal")]
mod rust_decimal_impl {
    use diesel::sql_types;
    use rust_decimal::Decimal;

    use crate::Mssql;

    use super::unexpected_null;

    from_diesel_sql!(sql_types::Decimal, Decimal);
}

#[cfg(feature = "bigdecimal")]
mod bigdecimal_impl {
    use bigdecimal::BigDecimal;
    use diesel::sql_types;

    use crate::Mssql;

    use super::unexpected_null;

    from_diesel_sql!(sql_types::Decimal, BigDecimal);
}

#[cfg(feature = "chrono")]
mod chrono_impl {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use diesel::sql_types;

    use super::unexpected_null;

    from_diesel_sql!(sql_types::Date, NaiveDate);
    from_diesel_sql!(sql_types::Time, NaiveTime);
    from_diesel_sql!(sql_types::Timestamp, NaiveDateTime);
}

#[cfg(feature = "time")]
mod time_impl {
    use diesel::sql_types;
    use time::{Date, Time};

    use super::unexpected_null;

    from_diesel_sql!(sql_types::Date, Date);
    from_diesel_sql!(sql_types::Time, Time);
    from_diesel_sql!(sql_types::Timestamp, time::PrimitiveDateTime);
    from_diesel_sql!(sql_types::Timestamp, time::OffsetDateTime);
    from_diesel_sql!(
        crate::mssql::sql_types::DateTimeOffset,
        time::OffsetDateTime
    );
}
