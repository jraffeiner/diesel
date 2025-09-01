//! The Mssql backend

use super::connection::ColumnData;
use super::query_builder::{MssqlQueryBuilder, TdsBindCollector};
use crate::backend::*;
use crate::sql_types::TypeMetadata;

/// The Mssql backend
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Default)]
pub struct Mssql;

/// Represents possible types, that can be transmitted as via the
/// Mssql wire protocol
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
#[non_exhaustive]
pub enum MssqlType {
    /// A null Value
    Null,
    /// Variable Size Int
    Intn,
    /// 128bit GUID
    Guid,
    /// Variable Size Float
    Floatn,
    /// XML
    Xml,
    /// User defined Datatypes
    Udt,
    /// Variable Datatype
    SSVariant,
    /// Single Bit / Boolean
    Bit,
    /// 8bit Int
    Int1,
    /// 16bit Int
    Int2,
    /// 32bit Int
    Int4,
    /// 64bit Int
    Int8,
    /// Date and Time
    Datetime,
    /// Date and Time
    Datetime2,
    /// Date and Time
    Datetime4,
    /// Datetime with variable Precision
    Datetimen,
    /// Datetime with TZ offset and variable Precision
    DatetimeOffsetn,
    /// 32bit Float
    Float4,
    /// 64bit Float
    Float8,
    /// Money
    Money,
    /// Money
    Money4,
    /// Decimal with variable Precision
    Decimaln,
    /// Numerical with variable Precision
    Numericn,
    /// Bits
    Bitn,
    /// Var Sized Binary
    BigVarBin,
    /// Binary
    BigBinary,
    /// Binary
    Image,
    /// Date
    Daten,
    /// Time
    Timen,
    /// Variable sized Text
    BigVarChar,
    /// Text
    BigChar,
    /// Variable sized Text
    NVarchar,
    /// Text
    NChar,
    /// Text
    Text,
    /// Text
    NText,
}

impl Backend for Mssql {
    type QueryBuilder = MssqlQueryBuilder;
    type RawValue<'a> = ColumnData<'a>;
    type BindCollector<'a> = TdsBindCollector<'a>;
}

impl TypeMetadata for Mssql {
    type TypeMetadata = MssqlType;
    type MetadataLookup = ();
}

impl SqlDialect for Mssql {
    type ReturningClause = sql_dialect::returning_clause::DoesNotSupportReturningClause;

    type OnConflictClause = sql_dialect::on_conflict_clause::DoesNotSupportOnConflictClause;

    type InsertWithDefaultKeyword = sql_dialect::default_keyword_for_insert::IsoSqlDefaultKeyword;
    type BatchInsertSupport = sql_dialect::batch_insert_support::PostgresLikeBatchInsertSupport;
    type DefaultValueClauseForInsert = sql_dialect::default_value_clause::AnsiDefaultValueClause;

    type EmptyFromClauseSyntax = sql_dialect::from_clause_syntax::AnsiSqlFromClauseSyntax;
    type SelectStatementSyntax = sql_dialect::select_statement_syntax::AnsiSqlSelectStatement;

    type ExistsSyntax = sql_dialect::exists_syntax::AnsiSqlExistsSyntax;
    type ArrayComparison = sql_dialect::array_comparison::AnsiSqlArrayComparison;

    type ConcatClause = MssqlConcatClause;
    type AliasSyntax = sql_dialect::alias_syntax::AsAliasSyntax;

    type WindowFrameClauseGroupSupport =
        sql_dialect::window_frame_clause_group_support::IsoGroupWindowFrameUnit;

    type WindowFrameExclusionSupport =
        sql_dialect::window_frame_exclusion_support::FrameExclusionSupport;

    type AggregateFunctionExpressions =
        sql_dialect::aggregate_function_expressions::PostgresLikeAggregateFunctionExpressions;

    type BuiltInWindowFunctionRequireOrder =
        sql_dialect::built_in_window_function_require_order::NoOrderRequired;
}

impl DieselReserveSpecialization for Mssql {}
impl TrustedBackend for Mssql {}

#[derive(Debug, Clone, Copy)]
pub struct MssqlConcatClause;
