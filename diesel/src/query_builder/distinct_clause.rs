use crate::backend::DieselReserveSpecialization;
#[cfg(feature = "mssql")]
use crate::mssql::Mssql;
use crate::query_builder::*;
use crate::query_dsl::group_by_dsl::ValidDistinctForGroupBy;
use crate::query_dsl::order_dsl::ValidOrderingForDistinct;

#[derive(Debug, Clone, Copy, QueryId)]
pub struct NoDistinctClause;
#[derive(Debug, Clone, Copy, QueryId)]
pub struct DistinctClause;

impl<DB> QueryFragment<DB> for NoDistinctClause
where
    DB: Backend + DieselReserveSpecialization,
{
    fn walk_ast<'b>(&'b self, _: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        Ok(())
    }
}

#[cfg(feature = "mssql")]
impl QueryFragment<Mssql> for NoDistinctClause {
    fn walk_ast<'b>(&'b self, _: AstPass<'_, 'b, Mssql>) -> QueryResult<()> {
        Ok(())
    }
}

impl<DB> QueryFragment<DB> for DistinctClause
where
    DB: Backend + DieselReserveSpecialization,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        out.push_sql("DISTINCT ");
        Ok(())
    }
}

#[cfg(feature = "mssql")]
impl QueryFragment<Mssql> for DistinctClause {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Mssql>) -> QueryResult<()> {
        out.push_sql("DISTINCT ");
        Ok(())
    }
}

impl<O> ValidOrderingForDistinct<NoDistinctClause> for O {}
impl<O> ValidOrderingForDistinct<DistinctClause> for O {}
impl<S, G> ValidDistinctForGroupBy<S, G> for NoDistinctClause {}
impl<S, G> ValidDistinctForGroupBy<S, G> for DistinctClause {}

// This is rexported from another location
#[expect(unreachable_pub, unused_imports)]
#[cfg(feature = "postgres_backend")]
pub use crate::pg::DistinctOnClause;
