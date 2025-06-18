//! The Firebird backend

use super::query_builder::FbQueryBuilder;
use super::types::SupportedType;
use super::value::FbValue;
use crate::backend::*;
use crate::query_builder::bind_collector::RawBytesBindCollector;
use crate::sql_types::TypeMetadata;

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct Fb;

impl Backend for Fb {
    type QueryBuilder = FbQueryBuilder;

    type RawValue<'a> = FbValue<'a>;

    type BindCollector<'a> = RawBytesBindCollector<Fb>;
}

impl TrustedBackend for Fb {}
impl DieselReserveSpecialization for Fb {}

impl TypeMetadata for Fb {
    type TypeMetadata = SupportedType;
    // TODO: add firebird domains support
    type MetadataLookup = ();
}

#[allow(
    missing_docs,
    missing_copy_implementations,
    missing_debug_implementations
)]
pub struct FbSelectStatementSyntax;

#[derive(Debug, Copy, Clone)]
#[allow(missing_docs)]
pub struct FbReturningClause;

impl SqlDialect for Fb {
    type ReturningClause = FbReturningClause;

    type ConcatClause = sql_dialect::concat_clause::ConcatWithPipesClause;

    type OnConflictClause = sql_dialect::on_conflict_clause::DoesNotSupportOnConflictClause;

    type InsertWithDefaultKeyword =
        sql_dialect::default_keyword_for_insert::DoesNotSupportDefaultKeyword;

    type BatchInsertSupport = sql_dialect::batch_insert_support::DoesNotSupportBatchInsert;

    type DefaultValueClauseForInsert = sql_dialect::default_value_clause::AnsiDefaultValueClause;

    type EmptyFromClauseSyntax = sql_dialect::from_clause_syntax::AnsiSqlFromClauseSyntax;

    type ExistsSyntax = sql_dialect::exists_syntax::AnsiSqlExistsSyntax;

    type ArrayComparison = sql_dialect::array_comparison::AnsiSqlArrayComparison;
    type SelectStatementSyntax = FbSelectStatementSyntax;

    type AliasSyntax = sql_dialect::alias_syntax::AsAliasSyntax;
}
