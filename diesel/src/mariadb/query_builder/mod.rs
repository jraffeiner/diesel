use crate::mariadb::Mariadb;

/// The Mariadb query builder
pub type MariadbQueryBuilder = crate::mysql::query_builder::MysqlLikeQueryBuilder<Mariadb>;

mod limit_offset;
mod query_fragment_impls;
