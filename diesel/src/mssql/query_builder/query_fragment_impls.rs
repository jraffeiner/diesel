use crate::{
    expression::operators::Concat,
    mssql::Mssql,
    query_builder::{
        IntoBoxedClause, QueryFragment, limit_clause::{LimitClause, NoLimitClause}, limit_offset_clause::{BoxedLimitOffsetClause, LimitOffsetClause}, offset_clause::{NoOffsetClause, OffsetClause}
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

impl QueryFragment<Mssql> for BoxedLimitOffsetClause<'_, Mssql> {
    fn walk_ast<'b>(&'b self, mut pass: diesel::query_builder::AstPass<'_, 'b, Mssql>) -> diesel::QueryResult<()> {
        match (self.limit.as_ref(), self.offset.as_ref()) {
            (Some(limit), Some(offset)) => {
                pass.push_sql(" OFFSET ");
                offset.walk_ast(pass.reborrow())?;
                pass.push_sql(" ROWS FETCH NEXT ");
                limit.walk_ast(pass.reborrow())?;
                pass.push_sql(" ROWS ONLY ");
            }
            (Some(limit), None) => {
                pass.push_sql(" OFFSET 0 ROWS FETCH NEXT ");
                limit.walk_ast(pass.reborrow())?;
                pass.push_sql(" ROWS ONLY ");
            }
            (None, Some(offset)) => {
                pass.push_sql(" OFFSET ");
                offset.walk_ast(pass.reborrow())?;
            }
            (None, None) => {}
        }
        Ok(())
    }
}

impl<'a> IntoBoxedClause<'a, Mssql> for LimitOffsetClause<NoLimitClause, NoOffsetClause> {
    type BoxedClause = BoxedLimitOffsetClause<'a, Mssql>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: None,
            offset: None,
        }
    }
}

impl<'a, L> IntoBoxedClause<'a, Mssql> for LimitOffsetClause<LimitClause<L>, NoOffsetClause>
where
    L: QueryFragment<Mssql> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, Mssql>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: Some(Box::new(self.limit_clause)),
            offset: None,
        }
    }
}

impl<'a, L, O> IntoBoxedClause<'a, Mssql> for LimitOffsetClause<LimitClause<L>, OffsetClause<O>>
where
    L: QueryFragment<Mssql> + Send + 'a,
    O: QueryFragment<Mssql> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, Mssql>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: Some(Box::new(self.limit_clause)),
            offset: Some(Box::new(self.offset_clause)),
        }
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
