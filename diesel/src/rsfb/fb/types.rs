//! Types implementation of Firebird support

use super::backend::Fb;
use super::value::FbValue;
use crate::deserialize::{self, FromSql};
use crate::result::Error::DatabaseError;
use crate::result::*;
use crate::serialize::{self, IsNull, ToSql};
use crate::sql_types::{self, HasSqlType};
use bytes::Buf;
use bytes::Bytes;
#[cfg(feature = "chrono")]
use chrono::*;
use rsfbclient::{ColumnToVal, IntoParam, SqlType};
use std::boxed::Box;
use std::error::Error;
use std::io::Write;
#[cfg(feature = "time")]
use time::*;

/// Supported types by the diesel
/// Firebird implementation
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
pub enum SupportedType {
    Text,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Date,
    Time,
    DateTime,
    Bool,
    Blob,
}

#[allow(missing_docs)]
impl SupportedType {
    pub fn into_param(self, source_val: Option<Vec<u8>>) -> SqlType {
        if let Some(val) = source_val {
            #[expect(unreachable_patterns)]
            match self {
                SupportedType::Text => String::from_utf8(val).expect("Invalid UTF-8").into_param(),
                SupportedType::SmallInt => Bytes::copy_from_slice(&val).get_i16().into_param(),
                SupportedType::Int => Bytes::copy_from_slice(&val).get_i32().into_param(),
                SupportedType::BigInt => Bytes::copy_from_slice(&val).get_i64().into_param(),
                SupportedType::Float => Bytes::copy_from_slice(&val).get_f32().into_param(),
                SupportedType::Double => Bytes::copy_from_slice(&val).get_f64().into_param(),
                SupportedType::Date => {
                    let days = Bytes::copy_from_slice(&val).get_i32();

                    #[cfg(feature = "chrono")]
                    NaiveDate::from_num_days_from_ce_opt(days).into_param()
                }
                SupportedType::Time => {
                    let secs = Bytes::copy_from_slice(&val).get_u32();

                    #[cfg(feature = "chrono")]
                    NaiveTime::from_num_seconds_from_midnight_opt(secs, 0).into_param()
                }
                SupportedType::DateTime => {
                    let tms = Bytes::copy_from_slice(&val).get_i64();

                    #[cfg(feature = "chrono")]
                    DateTime::from_timestamp(tms, 0)
                        .map(|s| s.naive_utc())
                        .into_param()
                }
                SupportedType::Bool => {
                    let bo = Bytes::copy_from_slice(&val).get_i8() == 1;
                    bo.into_param()
                }
                SupportedType::Blob => val.into_param(),
                _ => SqlType::Null,
            }
        } else {
            SqlType::Null
        }
    }
}

impl HasSqlType<sql_types::SmallInt> for Fb {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        SupportedType::SmallInt
    }
}

impl HasSqlType<sql_types::Integer> for Fb {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        SupportedType::Int
    }
}

impl HasSqlType<sql_types::BigInt> for Fb {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        SupportedType::BigInt
    }
}

impl HasSqlType<sql_types::Float> for Fb {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        SupportedType::Float
    }
}

impl HasSqlType<sql_types::Double> for Fb {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        SupportedType::Double
    }
}

impl HasSqlType<sql_types::VarChar> for Fb {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        SupportedType::Text
    }
}

impl HasSqlType<sql_types::Binary> for Fb {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        SupportedType::Blob
    }
}

impl HasSqlType<sql_types::Date> for Fb {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        SupportedType::Date
    }
}

impl HasSqlType<sql_types::Time> for Fb {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        SupportedType::Time
    }
}

impl HasSqlType<sql_types::Timestamp> for Fb {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        SupportedType::DateTime
    }
}

impl HasSqlType<sql_types::Bool> for Fb {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        SupportedType::Bool
    }
}

impl FromSql<sql_types::Integer, Fb> for i32 {
    fn from_sql(value: FbValue<'_>) -> deserialize::Result<Self> {
        let rs = value
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(rs)
    }
}

impl FromSql<sql_types::VarChar, Fb> for String {
    fn from_sql(value: FbValue<'_>) -> deserialize::Result<Self> {
        let rs = value
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(rs)
    }
}

impl FromSql<sql_types::Float, Fb> for f32 {
    fn from_sql(value: FbValue<'_>) -> deserialize::Result<Self> {
        let rs = value
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(rs)
    }
}

#[cfg(feature = "chrono")]
impl FromSql<sql_types::Date, Fb> for NaiveDate {
    fn from_sql(value: FbValue<'_>) -> deserialize::Result<Self> {
        let rs = value
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(rs)
    }
}

#[cfg(feature = "time")]
impl FromSql<sql_types::Date, Fb> for Date {
    fn from_sql(bytes: <Fb as crate::backend::Backend>::RawValue<'_>) -> deserialize::Result<Self> {
        let rs: NaiveDate = bytes
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;
        let td = Date::from_ordinal_date(rs.year(), rs.ordinal() as u16).expect("valid date");
        Ok(td)
    }
}

#[cfg(feature = "chrono")]
impl ToSql<sql_types::Date, Fb> for NaiveDate {
    fn to_sql<'b>(&self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let days = self.num_days_from_ce().to_be_bytes();
        out.write_all(&days)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }
}

#[cfg(feature = "time")]
impl ToSql<sql_types::Date, Fb> for Date {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let naive = NaiveDate::from_yo_opt(self.year(), self.ordinal() as u32).expect("valid date");
        let days = naive.num_days_from_ce().to_be_bytes();
        out.write_all(&days)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }
}

#[cfg(feature = "chrono")]
impl FromSql<sql_types::Timestamp, Fb> for NaiveDateTime {
    fn from_sql(value: FbValue<'_>) -> deserialize::Result<Self> {
        let rs = value
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(rs)
    }
}

#[cfg(feature = "time")]
impl FromSql<sql_types::Timestamp, Fb> for PrimitiveDateTime {
    fn from_sql(bytes: <Fb as crate::backend::Backend>::RawValue<'_>) -> deserialize::Result<Self> {
        let rs: NaiveDateTime = bytes
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;
        let pdt = PrimitiveDateTime::new(
            Date::from_ordinal_date(rs.year(), rs.ordinal() as u16).expect("valid date"),
            Time::from_hms_nano(
                rs.hour() as u8,
                rs.minute() as u8,
                rs.second() as u8,
                rs.nanosecond(),
            )
            .expect("valid time"),
        );
        Ok(pdt)
    }
}

#[cfg(feature = "chrono")]
impl ToSql<sql_types::Timestamp, Fb> for NaiveDateTime {
    fn to_sql<'b>(&self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let tms = self.and_utc().timestamp().to_be_bytes();
        out.write_all(&tms)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }
}

#[cfg(feature = "time")]
impl ToSql<sql_types::Timestamp, Fb> for PrimitiveDateTime {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let tms = self.assume_utc().unix_timestamp().to_be_bytes();
        out.write_all(&tms)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }
}

#[cfg(feature = "chrono")]
impl FromSql<sql_types::Time, Fb> for NaiveTime {
    fn from_sql(value: FbValue<'_>) -> deserialize::Result<Self> {
        let rs = value
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(rs)
    }
}

#[cfg(feature = "time")]
impl FromSql<sql_types::Time, Fb> for Time {
    fn from_sql(bytes: <Fb as crate::backend::Backend>::RawValue<'_>) -> deserialize::Result<Self> {
        let rs: NaiveTime = bytes
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;
        let time = Time::from_hms(rs.hour() as u8, rs.minute() as u8, rs.second() as u8)
            .expect("valid time");
        Ok(time)
    }
}

#[cfg(feature = "chrono")]
impl ToSql<sql_types::Time, Fb> for NaiveTime {
    fn to_sql<'b>(&self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let secs = self.num_seconds_from_midnight().to_be_bytes();
        out.write_all(&secs)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }
}

#[cfg(feature = "time")]
impl ToSql<sql_types::Time, Fb> for Time {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let secs = NaiveTime::from_hms_opt(
            self.hour() as u32,
            self.minute() as u32,
            self.second() as u32,
        )
        .expect("valid time")
        .num_seconds_from_midnight()
        .to_be_bytes();
        out.write_all(&secs)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }
}

impl FromSql<sql_types::Bool, Fb> for bool {
    fn from_sql(value: FbValue<'_>) -> deserialize::Result<Self> {
        let rs = value
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(rs)
    }
}

impl ToSql<sql_types::Bool, Fb> for bool {
    fn to_sql<'b>(&self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let bo = (*self as i8).to_be_bytes();
        out.write_all(&bo)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }
}

impl FromSql<sql_types::Binary, Fb> for Vec<u8> {
    fn from_sql(value: FbValue<'_>) -> deserialize::Result<Self> {
        let rs = value
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(rs)
    }
}

impl ToSql<sql_types::Integer, Fb> for i32 {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let i = self.to_be_bytes();

        out.write_all(&i)
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::BigInt, Fb> for i64 {
    fn from_sql(value: FbValue<'_>) -> deserialize::Result<Self> {
        let rs = value
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(rs)
    }
}

impl ToSql<sql_types::BigInt, Fb> for i64 {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let i = self.to_be_bytes();

        out.write_all(&i)
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(IsNull::No)
    }
}

impl ToSql<sql_types::Float, Fb> for f32 {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let i = self.to_be_bytes();

        out.write_all(&i)
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::Double, Fb> for f64 {
    fn from_sql(value: FbValue<'_>) -> deserialize::Result<Self> {
        let rs = value
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(rs)
    }
}

impl ToSql<sql_types::Double, Fb> for f64 {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let i = self.to_be_bytes();

        out.write_all(&i)
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::SmallInt, Fb> for i16 {
    fn from_sql(value: FbValue<'_>) -> deserialize::Result<Self> {
        let rs = value
            .raw
            .clone()
            .to_val()
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(rs)
    }
}

impl ToSql<sql_types::SmallInt, Fb> for i16 {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Fb>) -> serialize::Result {
        let i = self.to_be_bytes();

        out.write_all(&i)
            .map_err(|e| DatabaseError(DatabaseErrorKind::Unknown, Box::new(e.to_string())))?;

        Ok(IsNull::No)
    }
}
