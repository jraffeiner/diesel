mod date_and_time;
mod decimal;
#[cfg(all(
    feature = "serde_json",
    any(
        feature = "postgres_backend",
        feature = "mysql_backend",
        feature = "sqlite"
    )
))]
mod json;
mod option;
mod primitives;
pub(crate) mod tuples;

#[cfg(all(any(feature = "postgres_backend", feature = "mssql_backend"), feature = "uuid"))]
mod uuid;
