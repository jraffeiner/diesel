use crate::{
    mssql::{
        connection::{
            error::Error,
            tds::codec::{ColumnData, FixedLenType, TokenRow, TypeInfo, VarLenType},
        },
        Mssql,
    },
    row::{Field, PartialRow, RowIndex, RowSealed},
    QueryResult,
};
use std::{fmt::Display, sync::Arc};

use super::{from_sql::FromSql, QueryStream};

/// A column of data from a query.
#[derive(Debug, Clone)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
}

impl Column {
    /// Construct a new Column.
    pub fn new(name: String, column_type: ColumnType) -> Self {
        Self { name, column_type }
    }

    /// The name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The type of the column.
    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The type of the column.
pub enum ColumnType {
    /// The column doesn't have a specified type.
    Null,
    /// A bit or boolean value.
    Bit,
    /// An 8-bit integer value.
    Int1,
    /// A 16-bit integer value.
    Int2,
    /// A 32-bit integer value.
    Int4,
    /// A 64-bit integer value.
    Int8,
    /// A 32-bit datetime value.
    Datetime4,
    /// A 32-bit floating point value.
    Float4,
    /// A 64-bit floating point value.
    Float8,
    /// Money value.
    Money,
    /// A TDS 7.2 datetime value.
    Datetime,
    /// A 32-bit money value.
    Money4,
    /// A unique identifier, UUID.
    Guid,
    /// N-bit integer value (variable).
    Intn,
    /// A bit value in a variable-length type.
    Bitn,
    /// A decimal value (same as `Numericn`).
    Decimaln,
    /// A numeric value (same as `Decimaln`).
    Numericn,
    /// A n-bit floating point value.
    Floatn,
    /// A n-bit datetime value (TDS 7.2).
    Datetimen,
    /// A n-bit date value (TDS 7.3).
    Daten,
    /// A n-bit time value (TDS 7.3).
    Timen,
    /// A n-bit datetime2 value (TDS 7.3).
    Datetime2,
    /// A n-bit datetime value with an offset (TDS 7.3).
    DatetimeOffsetn,
    /// A variable binary value.
    BigVarBin,
    /// A large variable string value.
    BigVarChar,
    /// A binary value.
    BigBinary,
    /// A string value.
    BigChar,
    /// A variable string value with UTF-16 encoding.
    NVarchar,
    /// A string value with UTF-16 encoding.
    NChar,
    /// A XML value.
    Xml,
    /// User-defined type.
    Udt,
    /// A text value (deprecated).
    Text,
    /// A image value (deprecated).
    Image,
    /// A text value with UTF-16 encoding (deprecated).
    NText,
    /// An SQL variant type.
    SSVariant,
}

impl From<&TypeInfo> for ColumnType {
    fn from(ti: &TypeInfo) -> Self {
        match ti {
            TypeInfo::FixedLen(flt) => match flt {
                FixedLenType::Int1 => Self::Int1,
                FixedLenType::Bit => Self::Bit,
                FixedLenType::Int2 => Self::Int2,
                FixedLenType::Int4 => Self::Int4,
                FixedLenType::Datetime4 => Self::Datetime4,
                FixedLenType::Float4 => Self::Float4,
                FixedLenType::Money => Self::Money,
                FixedLenType::Datetime => Self::Datetime,
                FixedLenType::Float8 => Self::Float8,
                FixedLenType::Money4 => Self::Money4,
                FixedLenType::Int8 => Self::Int8,
                FixedLenType::Null => Self::Null,
            },
            TypeInfo::VarLenSized(cx) => match cx.r#type() {
                VarLenType::Guid => Self::Guid,
                VarLenType::Intn => match cx.len() {
                    1 => Self::Int1,
                    2 => Self::Int2,
                    4 => Self::Int4,
                    8 => Self::Int8,
                    _ => Self::Intn,
                },
                VarLenType::Bitn => Self::Bitn,
                VarLenType::Decimaln => Self::Decimaln,
                VarLenType::Numericn => Self::Numericn,
                VarLenType::Floatn => match cx.len() {
                    4 => Self::Float4,
                    8 => Self::Float8,
                    _ => Self::Floatn,
                },
                VarLenType::Money => Self::Money,
                VarLenType::Datetimen => Self::Datetimen,
                VarLenType::Daten => Self::Daten,
                VarLenType::Timen => Self::Timen,
                VarLenType::Datetime2 => Self::Datetime2,
                VarLenType::DatetimeOffsetn => Self::DatetimeOffsetn,
                VarLenType::BigVarBin => Self::BigVarBin,
                VarLenType::BigVarChar => Self::BigVarChar,
                VarLenType::BigBinary => Self::BigBinary,
                VarLenType::BigChar => Self::BigChar,
                VarLenType::NVarchar => Self::NVarchar,
                VarLenType::NChar => Self::NChar,
                VarLenType::Xml => Self::Xml,
                VarLenType::Udt => Self::Udt,
                VarLenType::Text => Self::Text,
                VarLenType::Image => Self::Image,
                VarLenType::NText => Self::NText,
                VarLenType::SSVariant => Self::SSVariant,
            },
            TypeInfo::VarLenSizedPrecision { ty, .. } => match ty {
                VarLenType::Guid => Self::Guid,
                VarLenType::Intn => Self::Intn,
                VarLenType::Bitn => Self::Bitn,
                VarLenType::Decimaln => Self::Decimaln,
                VarLenType::Numericn => Self::Numericn,
                VarLenType::Floatn => Self::Floatn,
                VarLenType::Money => Self::Money,
                VarLenType::Datetimen => Self::Datetimen,
                VarLenType::Daten => Self::Daten,
                VarLenType::Timen => Self::Timen,
                VarLenType::Datetime2 => Self::Datetime2,
                VarLenType::DatetimeOffsetn => Self::DatetimeOffsetn,
                VarLenType::BigVarBin => Self::BigVarBin,
                VarLenType::BigVarChar => Self::BigVarChar,
                VarLenType::BigBinary => Self::BigBinary,
                VarLenType::BigChar => Self::BigChar,
                VarLenType::NVarchar => Self::NVarchar,
                VarLenType::NChar => Self::NChar,
                VarLenType::Xml => Self::Xml,
                VarLenType::Udt => Self::Udt,
                VarLenType::Text => Self::Text,
                VarLenType::Image => Self::Image,
                VarLenType::NText => Self::NText,
                VarLenType::SSVariant => Self::SSVariant,
            },
            TypeInfo::Xml { .. } => Self::Xml,
        }
    }
}

/// A row of data from a query.
///
/// Data can be accessed either by copying through [`get`] or [`try_get`]
/// methods, or moving by value using the [`IntoIterator`] implementation.
///
/// ```
/// # use tiberius::{Config, FromSqlOwned};
/// # use std::env;
/// #
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
/// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
/// # );
/// # let config = Config::from_ado_string(&c_str)?;
/// # let tcp = std::net::TcpStream::connect(config.get_addr())?;
/// # tcp.set_nodelay(true)?;
/// # let mut client = tiberius::Client::connect(config, tcp)?;
/// // by-reference
/// let row = client
///     .query("SELECT @P1 AS col1", &[&"test"])
///     ?
///     .into_row()
///     ?
///     .unwrap();
///
/// assert_eq!(Some("test"), row.get("col1"));
///
/// // ...or by-value
/// let row = client
///     .query("SELECT @P1 AS col1", &[&"test"])
///     ?
///     .into_row()
///     ?
///     .unwrap();
///
/// for val in row.into_iter() {
///     assert_eq!(
///         Some(String::from("test")),
///         String::from_sql_owned(val)?
///     )
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`get`]: #method.get
/// [`try_get`]: #method.try_get
/// [`IntoIterator`]: #impl-IntoIterator
#[derive(Debug)]
pub struct MssqlRow {
    pub(crate) columns: Arc<Vec<Column>>,
    pub(crate) data: TokenRow<'static>,
    pub(crate) result_index: usize,
}

pub trait QueryIdx
where
    Self: Display,
{
    fn idx(&self, row: &MssqlRow) -> Option<usize>;
}

impl QueryIdx for usize {
    fn idx(&self, _row: &MssqlRow) -> Option<usize> {
        Some(*self)
    }
}

impl QueryIdx for &str {
    fn idx(&self, row: &MssqlRow) -> Option<usize> {
        row.columns.iter().position(|c| c.name() == *self)
    }
}

impl MssqlRow {
    /// Columns defining the row data. Columns listed here are in the same order
    /// as the resulting data.
    ///
    /// # Example
    ///
    /// ```
    /// # use tiberius::Config;
    /// # use std::env;
    /// #
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = std::net::TcpStream::connect(config.get_addr())?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp)?;
    /// let row = client
    ///     .query("SELECT 1 AS foo, 2 AS bar", &[])
    ///     ?
    ///     .into_row()
    ///     ?
    ///     .unwrap();
    ///
    /// assert_eq!("foo", row.columns()[0].name());
    /// assert_eq!("bar", row.columns()[1].name());
    /// # Ok(())
    /// # }
    /// ```
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Return an iterator over row column-value pairs.
    pub fn cells(&self) -> impl Iterator<Item = (&Column, &ColumnData<'static>)> {
        self.columns().iter().zip(self.data.iter())
    }

    /// The result set number, starting from zero and increasing if the stream
    /// has results from more than one query.
    pub fn result_index(&self) -> usize {
        self.result_index
    }

    /// Returns the number of columns in the row.
    ///
    /// # Example
    ///
    /// ```
    /// # use tiberius::Config;
    /// # use std::env;
    /// #
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = std::net::TcpStream::connect(config.get_addr())?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp)?;
    /// let row = client
    ///     .query("SELECT 1, 2", &[])
    ///     ?
    ///     .into_row()
    ///     ?
    ///     .unwrap();
    ///
    /// assert_eq!(2, row.len());
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Retrieve a column value for a given column index, which can either be
    /// the zero-indexed position or the name of the column.
    ///
    /// # Example
    ///
    /// ```
    /// # use tiberius::Config;
    /// # use std::env;
    /// #
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = std::net::TcpStream::connect(config.get_addr())?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp)?;
    /// let row = client
    ///     .query("SELECT @P1 AS col1", &[&1i32])
    ///     ?
    ///     .into_row()
    ///     ?
    ///     .unwrap();
    ///
    /// assert_eq!(Some(1i32), row.get(0));
    /// assert_eq!(Some(1i32), row.get("col1"));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// - The requested type conversion (SQL->Rust) is not possible.
    /// - The given index is out of bounds (column does not exist).
    ///
    /// Use [`try_get`] for a non-panicking version of the function.
    ///
    /// [`try_get`]: #method.try_get
    #[track_caller]
    pub fn get<'a, R, I>(&'a self, idx: I) -> Option<R>
    where
        R: FromSql<'a>,
        I: QueryIdx,
    {
        self.try_get(idx).unwrap()
    }

    /// Retrieve a column's value for a given column index.
    #[track_caller]
    pub fn try_get<'a, R, I>(&'a self, idx: I) -> crate::mssql::connection::Result<Option<R>>
    where
        R: FromSql<'a>,
        I: QueryIdx,
    {
        let idx = idx.idx(self).ok_or_else(|| {
            Error::Conversion(format!("Could not find column with index {}", idx).into())
        })?;

        let data = self.data.get(idx).unwrap();

        R::from_sql(data)
    }
}

impl IntoIterator for MssqlRow {
    type Item = ColumnData<'static>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

#[derive(Debug)]
pub struct MssqlCursor {
    results: Vec<MssqlRow>,
}

impl Field<'_, Mssql> for ColumnData<'_> {
    fn field_name(&self) -> Option<&str> {
        None
    }

    fn value(&self) -> Option<<Mssql as diesel::backend::Backend>::RawValue<'_>> {
        if self.is_null() {
            None
        } else {
            Some(self.to_owned())
        }
    }
}

impl RowSealed for MssqlRow {}

impl<'b> RowIndex<&'b str> for MssqlRow {
    fn idx(&self, idx: &'b str) -> Option<usize> {
        self.columns().iter().position(|col| col.name() == idx)
    }
}

impl RowIndex<usize> for MssqlRow {
    fn idx(&self, idx: usize) -> Option<usize> {
        if idx >= self.len() {
            None
        } else {
            Some(idx)
        }
    }
}

impl<'conn> crate::row::Row<'conn, Mssql> for MssqlRow {
    type Field<'f> = ColumnData<'f> where 'conn: 'f, Self: 'f;

    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.columns.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'conn: 'b,
        Self: diesel::row::RowIndex<I>,
    {
        if let Some(idx) = self.idx(idx) {
            self.cells()
                .map(|(_, data)| data.to_owned().into())
                .nth(idx)
        } else {
            None
        }
    }

    fn partial_row(
        &self,
        range: std::ops::Range<usize>,
    ) -> diesel::row::PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

impl MssqlCursor {
    pub fn gather_stream(stream: QueryStream<'_>) -> QueryResult<MssqlCursor> {
        let results = stream
            .into_results()
            .map_err(|e| crate::result::Error::QueryBuilderError(Box::new(e)))?;
        let iterator = results
            .into_iter()
            .flat_map(IntoIterator::into_iter)
            .map(MssqlRow::from)
            .rev()
            .collect();
        Ok(MssqlCursor { results: iterator })
    }
}

impl Iterator for MssqlCursor {
    type Item = QueryResult<MssqlRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.results.pop().map(Ok)
    }
}
