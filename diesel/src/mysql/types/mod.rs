//! MySQL specific types

pub(super) mod date_and_time;
mod enum_;
#[cfg(feature = "serde_json")]
mod json;
mod numeric;
mod primitives;

use crate::deserialize::{self, FromSql};
#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
use crate::mysql::MysqlLikeBackend;
use crate::mysql::{MysqlType, MysqlValue};
use crate::query_builder::QueryId;
use crate::serialize::{self, IsNull, Output, ToSql};
use crate::sql_types::*;
use crate::sql_types::{self, ops::*};
use byteorder::{NativeEndian, WriteBytesExt};

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> ToSql<TinyInt, B> for i8 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, B>) -> serialize::Result {
        out.write_i8(*self).map(|_| IsNull::No).map_err(Into::into)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> FromSql<TinyInt, B> for i8 {
    fn from_sql(value: B::RawValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        Ok(i8::from_be_bytes([bytes[0]]))
    }
}

/// Represents the MySQL unsigned type.
#[derive(Debug, Clone, Copy, Default, SqlType, QueryId)]
#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
pub struct Unsigned<ST: 'static>(ST);

impl<T> Add for Unsigned<T>
where
    T: Add,
{
    type Rhs = Unsigned<T::Rhs>;
    type Output = Unsigned<T::Output>;
}

impl<T> Sub for Unsigned<T>
where
    T: Sub,
{
    type Rhs = Unsigned<T::Rhs>;
    type Output = Unsigned<T::Output>;
}

impl<T> Mul for Unsigned<T>
where
    T: Mul,
{
    type Rhs = Unsigned<T::Rhs>;
    type Output = Unsigned<T::Output>;
}

impl<T> Div for Unsigned<T>
where
    T: Div,
{
    type Rhs = Unsigned<T::Rhs>;
    type Output = Unsigned<T::Output>;
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> ToSql<Unsigned<TinyInt>, B> for u8 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, B>) -> serialize::Result {
        out.write_u8(*self)?;
        Ok(IsNull::No)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> FromSql<Unsigned<TinyInt>, B> for u8 {
    #[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)] // that's what we want
    fn from_sql(bytes: MysqlValue<'_>) -> deserialize::Result<Self> {
        let signed: i8 = FromSql::<TinyInt, B>::from_sql(bytes)?;
        Ok(signed as u8)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> ToSql<Unsigned<SmallInt>, B> for u16 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, B>) -> serialize::Result {
        out.write_u16::<NativeEndian>(*self)?;
        Ok(IsNull::No)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> FromSql<Unsigned<SmallInt>, B> for u16
where
    i32: deserialize::FromSql<sql_types::Integer, B>,
{
    #[allow(
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )] // that's what we want
    fn from_sql(bytes: MysqlValue<'_>) -> deserialize::Result<Self> {
        let signed: i32 = FromSql::<Integer, B>::from_sql(bytes)?;
        Ok(signed as u16)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> ToSql<Unsigned<Integer>, B> for u32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, B>) -> serialize::Result {
        out.write_u32::<NativeEndian>(*self)?;
        Ok(IsNull::No)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> FromSql<Unsigned<Integer>, B> for u32
where
    i64: deserialize::FromSql<sql_types::BigInt, B>,
{
    #[allow(
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )] // that's what we want
    fn from_sql(bytes: MysqlValue<'_>) -> deserialize::Result<Self> {
        let signed: i64 = FromSql::<BigInt, B>::from_sql(bytes)?;
        Ok(signed as u32)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> ToSql<Unsigned<BigInt>, B> for u64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, B>) -> serialize::Result {
        out.write_u64::<NativeEndian>(*self)?;
        Ok(IsNull::No)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> FromSql<Unsigned<BigInt>, B> for u64
where
    i64: deserialize::FromSql<sql_types::BigInt, B>,
{
    #[allow(
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )] // that's what we want
    fn from_sql(bytes: MysqlValue<'_>) -> deserialize::Result<Self> {
        let signed: i64 = FromSql::<BigInt, B>::from_sql(bytes)?;
        Ok(signed as u64)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> ToSql<Bool, B> for bool {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, B>) -> serialize::Result {
        let int_value = i32::from(*self);
        <i32 as ToSql<Integer, B>>::to_sql(&int_value, &mut out.reborrow())
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> FromSql<Bool, B> for bool {
    fn from_sql(bytes: MysqlValue<'_>) -> deserialize::Result<Self> {
        Ok(bytes.as_bytes().iter().any(|x| *x != 0))
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> ToSql<sql_types::SmallInt, B> for i16 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, B>) -> serialize::Result {
        out.write_i16::<NativeEndian>(*self)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<_>)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> ToSql<sql_types::Integer, B> for i32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, B>) -> serialize::Result {
        out.write_i32::<NativeEndian>(*self)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<_>)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> ToSql<sql_types::BigInt, B> for i64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, B>) -> serialize::Result {
        out.write_i64::<NativeEndian>(*self)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<_>)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> ToSql<sql_types::Double, B> for f64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, B>) -> serialize::Result {
        out.write_f64::<NativeEndian>(*self)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<_>)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> ToSql<sql_types::Float, B> for f32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, B>) -> serialize::Result {
        out.write_f32::<NativeEndian>(*self)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<_>)
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> HasSqlType<Unsigned<TinyInt>> for B {
    fn metadata(_lookup: &mut ()) -> MysqlType {
        MysqlType::UnsignedTiny
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> HasSqlType<Unsigned<SmallInt>> for B {
    fn metadata(_lookup: &mut ()) -> MysqlType {
        MysqlType::UnsignedShort
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> HasSqlType<Unsigned<Integer>> for B {
    fn metadata(_lookup: &mut ()) -> MysqlType {
        MysqlType::UnsignedLong
    }
}

#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
impl<B: MysqlLikeBackend> HasSqlType<Unsigned<BigInt>> for B {
    fn metadata(_lookup: &mut ()) -> MysqlType {
        MysqlType::UnsignedLongLong
    }
}

/// Represents the MySQL datetime type.
///
/// ### [`ToSql`] impls
///
/// - [`chrono::NaiveDateTime`] with `feature = "chrono"`
/// - [`time::PrimitiveDateTime`] with `feature = "time"`
/// - [`time::OffsetDateTime`] with `feature = "time"`
///
/// ### [`FromSql`] impls
///
/// - [`chrono::NaiveDateTime`] with `feature = "chrono"`
/// - [`time::PrimitiveDateTime`] with `feature = "time"`
/// - [`time::OffsetDateTime`] with `feature = "time"`
///
/// [`ToSql`]: crate::serialize::ToSql
/// [`FromSql`]: crate::deserialize::FromSql
#[cfg_attr(
    feature = "chrono",
    doc = " [`chrono::NaiveDateTime`]: chrono::naive::NaiveDateTime"
)]
#[cfg_attr(
    not(feature = "chrono"),
    doc = " [`chrono::NaiveDateTime`]: https://docs.rs/chrono/0.4.19/chrono/naive/struct.NaiveDateTime.html"
)]
#[cfg_attr(
    feature = "time",
    doc = " [`time::PrimitiveDateTime`]: time::PrimitiveDateTime"
)]
#[cfg_attr(
    not(feature = "time"),
    doc = " [`time::PrimitiveDateTime`]: https://docs.rs/time/0.3.9/time/struct.PrimitiveDateTime.html"
)]
#[cfg_attr(
    feature = "time",
    doc = " [`time::OffsetDateTime`]: time::OffsetDateTime"
)]
#[cfg_attr(
    not(feature = "time"),
    doc = " [`time::OffsetDateTime`]: https://docs.rs/time/0.3.9/time/struct.OffsetDateTime.html"
)]
#[derive(Debug, Clone, Copy, Default, QueryId, SqlType)]
#[diesel(mysql_type(name = "DateTime"))]
#[cfg(any(feature = "mysql_backend", feature = "mariadb_backend"))]
pub struct Datetime;
