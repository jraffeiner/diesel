//! Provides types and functions related to working with Mssql
//!
//! Much of this module is re-exported from database agnostic locations.
//! However, if you are writing code specifically to extend Diesel on
//! Mssql, you may need to work with this module directly.

pub(crate) mod backend;
#[cfg(feature = "mssql")]
mod connection;
mod value;

pub(crate) mod query_builder;

pub use self::backend::{Mssql, MssqlType};
#[cfg(feature = "mssql")]
pub use self::connection::MssqlConnection;
pub use self::query_builder::MssqlQueryBuilder;

/// Mssql specific sql types
pub mod sql_types {
    use diesel_derives::{QueryId, SqlType};

    /// Mssql Specifig DateTimeOffset
    #[derive(Debug, Clone, Copy, Default, QueryId, SqlType)]
    #[diesel(mssql_type(name = "DatetimeOffsetn"))]
    pub struct DateTimeOffset;
}
