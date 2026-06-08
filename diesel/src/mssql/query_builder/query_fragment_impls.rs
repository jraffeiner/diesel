use crate::{
    AppearsOnTable, Expression, QuerySource, SelectableExpression, Table, backend::Backend, expression::{ValidGrouping, operators::Concat}, mssql::Mssql, query_builder::{
        AsQuery, FromClause, IntoBoxedClause, QueryFragment, QueryId, SelectStatement, limit_clause::{LimitClause, NoLimitClause}, limit_offset_clause::{BoxedLimitOffsetClause, LimitOffsetClause}, offset_clause::{NoOffsetClause, OffsetClause}, returning_clause::ReturningClause
    }
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
    fn walk_ast<'b>(
        &'b self,
        mut pass: diesel::query_builder::AstPass<'_, 'b, Mssql>,
    ) -> diesel::QueryResult<()> {
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

impl<E> QueryFragment<Mssql, crate::mssql::backend::MssqlOutputClause> for ReturningClause<E>
where E: QueryFragment<Mssql>,
{
    fn walk_ast<'b>(&'b self, mut pass: crate::query_builder::AstPass<'_, 'b, Mssql>) -> crate::prelude::QueryResult<()> {
        pass.push_sql(" OUTPUT ");
        self.0.walk_ast(pass.reborrow())
    }
}

/// Adds table hine `NO LOCK` to query table, columns need to be wrapped in `NoLockMarker<T>`
#[derive(Debug, Clone, Copy)]
pub struct NoLock<T>(T);

impl<T: QueryId> QueryId for NoLock<T>
where
    T: QueryId,
{
    type QueryId = T::QueryId;
    const HAS_STATIC_QUERY_ID: bool = <T as QueryId>::HAS_STATIC_QUERY_ID;
    const IS_WINDOW_FUNCTION: bool = T::IS_WINDOW_FUNCTION;
}

impl<T> diesel::QuerySource for NoLock<T>
where
    T: QuerySource,
    NoLockMarker<<T as QuerySource>::DefaultSelection>: SelectableExpression<NoLock<T>>,
    Self: Copy,
{
    type FromClause = Self;

    type DefaultSelection = NoLockMarker<T::DefaultSelection>;

    fn from_clause(&self) -> Self::FromClause {
        *self
    }

    fn default_selection(&self) -> Self::DefaultSelection {
        NoLockMarker(self.0.default_selection())
    }
}

impl<T> QueryFragment<Mssql> for NoLock<T>
where
    T: QueryFragment<Mssql>,
{
    fn walk_ast<'b>(
        &'b self,
        mut pass: diesel::query_builder::AstPass<'_, 'b, Mssql>,
    ) -> diesel::QueryResult<()> {
        self.0.walk_ast(pass.reborrow())?;
        pass.push_sql(" WITH (NOLOCK) ");
        Ok(())
    }
}

impl<T> AsQuery for NoLock<T>
where
    T: AsQuery,
    Self: QuerySource,
    <Self as QuerySource>::DefaultSelection: ValidGrouping<()>,
{
    type SqlType = <<Self as QuerySource>::DefaultSelection as Expression>::SqlType;

    type Query = SelectStatement<FromClause<Self>>;

    fn as_query(self) -> Self::Query {
        SelectStatement::simple(self)
    }
}

impl<T: Table + Copy> Table for NoLock<T>
where
    Self: QuerySource,
    Self: AsQuery,
    T: QuerySource,
    NoLockMarker<<T as Table>::AllColumns>: SelectableExpression<NoLock<T>>,
    NoLockMarker<<T as Table>::PrimaryKey>: SelectableExpression<NoLock<T>>,
    NoLockMarker<<T as QuerySource>::DefaultSelection>: SelectableExpression<NoLock<T>>,
{
    type PrimaryKey = NoLockMarker<T::PrimaryKey>;

    type AllColumns = NoLockMarker<T::AllColumns>;

    fn primary_key(&self) -> Self::PrimaryKey {
        NoLockMarker(self.0.primary_key())
    }

    fn all_columns() -> Self::AllColumns {
        NoLockMarker(T::all_columns())
    }
}

impl<EXPR, T> SelectableExpression<NoLock<T>> for NoLockMarker<EXPR> where
    EXPR: SelectableExpression<T>
{
}
impl<EXPR, T> AppearsOnTable<NoLock<T>> for NoLockMarker<EXPR> where EXPR: AppearsOnTable<T> {}

impl<E> Expression for NoLockMarker<E>
where
    E: Expression,
{
    type SqlType = E::SqlType;
}

/// Adds a `no_lock` method to tables, which adds `NO LOCK` to the query when used in a `FROM` clause. Columns need to be wrapped in `NoLockMarker<T>` for use with `NO LOCK`
pub trait WithNoLock: Sized {
    /// Adds `NO LOCK` to the query when used in a `FROM` clause. Columns need to be wrapped in `NoLockMarker<T>` for use with `NO LOCK`
    fn no_lock(self) -> NoLock<Self>;
}

impl<T> WithNoLock for T
where
    T: Table,
    NoLock<T>: QuerySource,
    NoLock<T>: AsQuery,
    T: QuerySource,
    NoLockMarker<<T as Table>::AllColumns>: SelectableExpression<NoLock<T>>,
    NoLockMarker<<T as Table>::PrimaryKey>: SelectableExpression<NoLock<T>>,
    NoLockMarker<<T as QuerySource>::DefaultSelection>: SelectableExpression<NoLock<T>>,
{
    fn no_lock(self) -> NoLock<Self> {
        NoLock(self)
    }
}

/// Columns need to wrapped with this struct for use with `NoLock<T>`
#[derive(Debug)]
pub struct NoLockMarker<T>(pub T);

impl<T, G> ValidGrouping<G> for NoLockMarker<T>
where
    T: ValidGrouping<G>,
{
    type IsAggregate = T::IsAggregate;
}

impl<T, DB> QueryFragment<DB> for NoLockMarker<T>
where
    T: QueryFragment<DB>,
    DB: Backend,
{
    fn walk_ast<'b>(
        &'b self,
        pass: crate::query_builder::AstPass<'_, 'b, DB>,
    ) -> crate::prelude::QueryResult<()> {
        self.0.walk_ast(pass)
    }
}

impl<T> QueryId for NoLockMarker<T>
where
    T: QueryId,
{
    type QueryId = T::QueryId;
    const HAS_STATIC_QUERY_ID: bool = T::HAS_STATIC_QUERY_ID;
    const IS_WINDOW_FUNCTION: bool = T::IS_WINDOW_FUNCTION;
}
