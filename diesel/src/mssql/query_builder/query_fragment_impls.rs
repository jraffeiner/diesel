use crate::{
    expression::operators::Concat,
    mssql::Mssql,
    query_builder::{
        limit_clause::{LimitClause, NoLimitClause},
        limit_offset_clause::LimitOffsetClause,
        offset_clause::{NoOffsetClause, OffsetClause},
        QueryFragment,
    },
};

impl QueryFragment<Mssql> for LimitOffsetClause<NoLimitClause, NoOffsetClause> {
    fn walk_ast<'b>(
        &'b self,
        _: diesel::query_builder::AstPass<'_, 'b, Mssql>,
    ) -> diesel::QueryResult<()> {
        Ok(())
    }
}

impl<L> QueryFragment<Mssql> for LimitOffsetClause<LimitClause<L>, NoOffsetClause>
where
    L: QueryFragment<Mssql>,
{
    fn walk_ast<'b>(
        &'b self,
        mut pass: diesel::query_builder::AstPass<'_, 'b, Mssql>,
    ) -> diesel::QueryResult<()> {
        pass.push_sql(" OFFSET 0 ROWS FETCH NEXT ");
        self.limit_clause.0.walk_ast(pass.reborrow())?;
        pass.push_sql(" ROWS ONLY ");
        Ok(())
    }
}

impl<L, O> QueryFragment<Mssql> for LimitOffsetClause<LimitClause<L>, OffsetClause<O>>
where
    L: QueryFragment<Mssql>,
    O: QueryFragment<Mssql>,
{
    fn walk_ast<'b>(
        &'b self,
        mut pass: diesel::query_builder::AstPass<'_, 'b, Mssql>,
    ) -> diesel::QueryResult<()> {
        pass.push_sql(" OFFSET ");
        self.offset_clause.0.walk_ast(pass.reborrow())?;
        pass.push_sql(" ROWS FETCH NEXT ");
        self.limit_clause.0.walk_ast(pass.reborrow())?;
        pass.push_sql(" ROWS ONLY ");
        Ok(())
    }
}

impl<L, R> QueryFragment<Mssql, crate::mssql::backend::MssqlConcatClause> for Concat<L, R>
where
    L: QueryFragment<Mssql>,
    R: QueryFragment<Mssql>,
{
    fn walk_ast<'b>(
        &'b self,
        mut pass: diesel::query_builder::AstPass<'_, 'b, Mssql>,
    ) -> diesel::QueryResult<()> {
        pass.push_sql(" CONCAT(");
        self.left.walk_ast(pass.reborrow())?;
        pass.push_sql(",");
        self.right.walk_ast(pass.reborrow())?;
        pass.push_sql(") ");
        Ok(())
    }
}
