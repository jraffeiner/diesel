//! Provides types and functions related to working with MySQL
//!
//! Much of this module is re-exported from database agnostic locations.
//! However, if you are writing code specifically to extend Diesel on
//! MySQL, you may need to work with this module directly.

pub(crate) mod backend;
#[cfg(any(feature = "mysql", feature = "mariadb"))]
mod connection;
mod value;

pub(crate) mod query_builder;
mod types;

use std::hash::Hash;

use crate::backend::{Backend, DieselReserveSpecialization};
use crate::mysql::query_builder::MysqlLikeQueryBuilder;
use crate::query_builder::bind_collector::RawBytesBindCollector;
use crate::sql_types::TypeMetadata;

pub use self::backend::{Mysql, MysqlType};
#[cfg(feature = "mysql")]
pub use self::connection::MysqlConnection;
pub use self::query_builder::MysqlQueryBuilder;
pub use self::value::{MysqlValue, NumericRepresentation};

#[cfg(feature = "mariadb")]
pub use self::connection::MysqlLikeConnection;

/// Data structures for MySQL types which have no corresponding Rust type
///
/// Most of these types are used to implement `ToSql` and `FromSql` for higher
/// level types.
pub mod data_types {
    #[doc(inline)]
    pub use super::types::date_and_time::{MysqlTime, MysqlTimestampType};
}

/// MySQL specific sql types
pub mod sql_types {
    #[doc(inline)]
    pub use super::types::{Datetime, Unsigned};
}

pub(crate) trait MysqlLikeBackend
where Self: for<'a> Backend<RawValue<'a> = MysqlValue<'a>, BindCollector<'a> = RawBytesBindCollector<Self>>,
    Self: TypeMetadata<TypeMetadata = MysqlType, MetadataLookup = ()>,
    Self: Backend<QueryBuilder = MysqlLikeQueryBuilder<Self>>,
    Self: Hash + Eq + Default,
    Self: DieselReserveSpecialization,
    Self: 'static,
{}

impl<B: Backend> MysqlLikeBackend for B
where B: for<'a> Backend<RawValue<'a> = MysqlValue<'a>, BindCollector<'a> = RawBytesBindCollector<B>>,
    B: TypeMetadata<TypeMetadata = MysqlType, MetadataLookup = ()>,
    B: Backend<QueryBuilder = MysqlLikeQueryBuilder<B>>,
    B: Hash + Eq + Default,
    B: DieselReserveSpecialization,
    B: 'static,
{}