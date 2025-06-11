mod binary;
mod bit;
mod bytes_mut_with_type_info;
mod date;
mod datetime2;
mod datetimen;
mod datetimeoffsetn;
mod fixed_len;
mod float;
mod guid;
mod image;
mod int;
mod money;
mod plp;
mod string;
mod text;
mod time;
mod var_len;
mod xml;

mod sql_variant;

use super::{Encode, FixedLenType, TypeInfo, VarLenType};
use crate::mssql::connection::tds::time::{Date, DateTime2, DateTimeOffset, Time};
use crate::mssql::connection::{
    tds::{time::DateTime, time::SmallDateTime, xml::XmlData, Numeric},
    SqlReadBytes,
};
use bytes::BufMut;
pub(crate) use bytes_mut_with_type_info::BytesMutWithTypeInfo;
use std::borrow::{BorrowMut, Cow};
use uuid::Uuid;

const MAX_NVARCHAR_SIZE: usize = 1 << 30;

#[derive(Clone, Debug, PartialEq)]
/// A container of a value that can be represented as a TDS value.
pub enum ColumnData<'a> {
    /// 8-bit integer, unsigned.
    U8(Option<u8>),
    /// 16-bit integer, signed.
    I16(Option<i16>),
    /// 32-bit integer, signed.
    I32(Option<i32>),
    /// 64-bit integer, signed.
    I64(Option<i64>),
    /// 32-bit floating point number.
    F32(Option<f32>),
    /// 64-bit floating point number.
    F64(Option<f64>),
    /// Boolean.
    Bit(Option<bool>),
    /// A string value.
    String(Option<Cow<'a, str>>),
    /// A Guid (UUID) value.
    Guid(Option<Uuid>),
    /// Binary data.
    Binary(Option<Cow<'a, [u8]>>),
    /// Numeric value (a decimal).
    Numeric(Option<Numeric>),
    /// XML data.
    Xml(Option<Cow<'a, XmlData>>),
    /// DateTime value.
    DateTime(Option<DateTime>),
    /// A small DateTime value.
    SmallDateTime(Option<SmallDateTime>),
    /// Time value.
    Time(Option<Time>),
    /// Date value.
    Date(Option<Date>),
    /// DateTime2 value.
    DateTime2(Option<DateTime2>),
    /// DateTime2 value with an offset.
    DateTimeOffset(Option<DateTimeOffset>),
    /// Special Variant acts as Placeholder for others
    SQLVariant(Option<&'a ColumnData<'a>>),
}

impl<'a> ColumnData<'a> {
    pub(crate) fn type_name(&self) -> Cow<'static, str> {
        match self {
            ColumnData::U8(_) => "tinyint".into(),
            ColumnData::I16(_) => "smallint".into(),
            ColumnData::I32(_) => "int".into(),
            ColumnData::I64(_) => "bigint".into(),
            ColumnData::F32(_) => "float(24)".into(),
            ColumnData::F64(_) => "float(53)".into(),
            ColumnData::Bit(_) => "bit".into(),
            ColumnData::String(None) => "nvarchar(4000)".into(),
            ColumnData::String(Some(ref s)) if s.len() <= 4000 => "nvarchar(4000)".into(),
            ColumnData::String(Some(ref s)) if s.len() <= MAX_NVARCHAR_SIZE => {
                "nvarchar(max)".into()
            }
            ColumnData::String(_) => "ntext(max)".into(),
            ColumnData::Guid(_) => "uniqueidentifier".into(),
            ColumnData::Binary(Some(ref b)) if b.len() <= 8000 => "varbinary(8000)".into(),
            ColumnData::Binary(_) => "varbinary(max)".into(),
            ColumnData::Numeric(Some(ref n)) => {
                format!("numeric({},{})", n.precision(), n.scale()).into()
            }
            ColumnData::Numeric(None) => "numeric".into(),
            ColumnData::Xml(_) => "xml".into(),
            ColumnData::DateTime(_) => "datetime".into(),
            ColumnData::SmallDateTime(_) => "smalldatetime".into(),
            ColumnData::Time(_) => "time".into(),
            ColumnData::Date(_) => "date".into(),
            ColumnData::DateTime2(_) => "datetime2".into(),
            ColumnData::DateTimeOffset(_) => "datetimeoffset".into(),
            ColumnData::SQLVariant(_) => "sql_variant".into(),
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            ColumnData::U8(None)
            | ColumnData::I16(None)
            | ColumnData::I32(None)
            | ColumnData::I64(None)
            | ColumnData::F32(None)
            | ColumnData::F64(None)
            | ColumnData::Bit(None)
            | ColumnData::String(None)
            | ColumnData::Guid(None)
            | ColumnData::Binary(None)
            | ColumnData::Numeric(None)
            | ColumnData::Xml(None)
            | ColumnData::DateTime(None)
            | ColumnData::SmallDateTime(None)
            | ColumnData::Time(None)
            | ColumnData::Date(None)
            | ColumnData::DateTime2(None)
            | ColumnData::DateTimeOffset(None)
            | ColumnData::SQLVariant(None) => true,
            _ => false,
        }
    }

    pub(crate) fn decode<R>(
        src: &mut R,
        ctx: &TypeInfo,
    ) -> crate::mssql::connection::Result<ColumnData<'a>>
    where
        R: SqlReadBytes,
    {
        let res = match ctx {
            TypeInfo::FixedLen(fixed_ty) => fixed_len::decode(src, fixed_ty)?,
            TypeInfo::VarLenSized(cx) => var_len::decode(src, cx)?,
            TypeInfo::VarLenSizedPrecision { ty, scale, .. } => match ty {
                VarLenType::Decimaln | VarLenType::Numericn => {
                    ColumnData::Numeric(Numeric::decode(src, *scale)?)
                }
                _ => todo!(),
            },
            TypeInfo::Xml { schema, size } => xml::decode(src, *size, schema.clone())?,
        };

        Ok(res)
    }
}

impl<'a> crate::mssql::connection::FromSql<'a> for ColumnData<'a> {
    fn from_sql(value: &'a ColumnData<'a>) -> crate::mssql::connection::Result<Option<Self>> {
        Ok(Some(Self::SQLVariant(Some(value))))
    }
}

impl<'a> Encode<BytesMutWithTypeInfo<'a>> for ColumnData<'a> {
    fn encode(self, dst: &mut BytesMutWithTypeInfo<'a>) -> crate::mssql::connection::Result<()> {
        match (self, dst.type_info()) {
            (ColumnData::Bit(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Bitn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(1);
                    dst.put_u8(val as u8);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::Bit(Some(val)), Some(TypeInfo::FixedLen(FixedLenType::Bit))) => {
                dst.put_u8(val as u8);
            }
            (ColumnData::Bit(Some(val)), None) => {
                // if TypeInfo was not given, encode a TypeInfo
                // the first 1 is part of TYPE_INFO
                // the second 1 is part of TYPE_VARBYTE
                let header = [VarLenType::Bitn as u8, 1, 1];
                dst.extend_from_slice(&header);
                dst.put_u8(val as u8);
            }
            (ColumnData::U8(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Intn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(1);
                    dst.put_u8(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::U8(Some(val)), Some(TypeInfo::FixedLen(FixedLenType::Int1))) => {
                dst.put_u8(val);
            }
            (ColumnData::U8(Some(val)), None) => {
                let header = [VarLenType::Intn as u8, 1, 1];
                dst.extend_from_slice(&header);
                dst.put_u8(val);
            }
            (ColumnData::I16(Some(val)), Some(TypeInfo::FixedLen(FixedLenType::Int2))) => {
                dst.put_i16_le(val);
            }
            (ColumnData::I16(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Intn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(2);
                    dst.put_i16_le(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::I16(Some(val)), None) => {
                let header = [VarLenType::Intn as u8, 2, 2];
                dst.extend_from_slice(&header);

                dst.put_i16_le(val);
            }
            (ColumnData::I32(Some(val)), Some(TypeInfo::FixedLen(FixedLenType::Int4))) => {
                dst.put_i32_le(val);
            }
            (ColumnData::I32(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Intn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(4);
                    dst.put_i32_le(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::I32(Some(val)), None) => {
                let header = [VarLenType::Intn as u8, 4, 4];
                dst.extend_from_slice(&header);
                dst.put_i32_le(val);
            }
            (ColumnData::I64(Some(val)), Some(TypeInfo::FixedLen(FixedLenType::Int8))) => {
                dst.put_i64_le(val);
            }
            (ColumnData::I64(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Intn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(8);
                    dst.put_i64_le(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::I64(Some(val)), None) => {
                let header = [VarLenType::Intn as u8, 8, 8];
                dst.extend_from_slice(&header);
                dst.put_i64_le(val);
            }
            (ColumnData::F32(Some(val)), Some(TypeInfo::FixedLen(FixedLenType::Float4))) => {
                dst.put_f32_le(val);
            }
            (ColumnData::F32(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Floatn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(4);
                    dst.put_f32_le(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::F32(Some(val)), None) => {
                let header = [VarLenType::Floatn as u8, 4, 4];
                dst.extend_from_slice(&header);
                dst.put_f32_le(val);
            }
            (ColumnData::F64(Some(val)), Some(TypeInfo::FixedLen(FixedLenType::Float8))) => {
                dst.put_f64_le(val);
            }
            (ColumnData::F64(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Floatn =>
            {
                if let Some(val) = opt {
                    dst.put_u8(8);
                    dst.put_f64_le(val);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::F64(Some(val)), None) => {
                let header = [VarLenType::Floatn as u8, 8, 8];
                dst.extend_from_slice(&header);
                dst.put_f64_le(val);
            }
            (ColumnData::Guid(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Guid =>
            {
                if let Some(uuid) = opt {
                    dst.put_u8(16);

                    let mut data = *uuid.as_bytes();
                    super::guid::reorder_bytes(&mut data);
                    dst.extend_from_slice(&data);
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::Guid(Some(uuid)), None) => {
                let header = [VarLenType::Guid as u8, 16, 16];
                dst.extend_from_slice(&header);

                let mut data = *uuid.as_bytes();
                super::guid::reorder_bytes(&mut data);
                dst.extend_from_slice(&data);
            }
            (ColumnData::String(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::BigChar
                    || vlc.r#type() == VarLenType::BigVarChar =>
            {
                if let Some(str) = opt {
                    let mut encoder = vlc.collation().as_ref().unwrap().encoding()?.new_encoder();
                    let len = encoder
                        .max_buffer_length_from_utf8_without_replacement(str.len())
                        .unwrap();
                    let mut bytes = Vec::with_capacity(len);
                    let (res, _) = encoder.encode_from_utf8_to_vec_without_replacement(
                        str.as_ref(),
                        &mut bytes,
                        true,
                    );
                    if let encoding_rs::EncoderResult::Unmappable(_) = res {
                        return Err(crate::mssql::connection::Error::Encoding(
                            "unrepresentable character".into(),
                        ));
                    }

                    if bytes.len() > vlc.len() {
                        return Err(crate::mssql::connection::Error::BulkInput(
                            format!(
                                "Encoded string length {} exceed column limit {}",
                                bytes.len(),
                                vlc.len()
                            )
                            .into(),
                        ));
                    }

                    if vlc.len() < 0xffff {
                        dst.put_u16_le(bytes.len() as u16);
                        dst.extend_from_slice(bytes.as_slice());
                    } else {
                        // unknown size
                        dst.put_u64_le(0xfffffffffffffffe);

                        assert!(
                            str.len() < 0xffffffff,
                            "if str longer than this, need to implement multiple blobs"
                        );

                        dst.put_u32_le(bytes.len() as u32);
                        dst.extend_from_slice(bytes.as_slice());

                        if !bytes.is_empty() {
                            // no next blob
                            dst.put_u32_le(0u32);
                        }
                    }
                } else if vlc.len() < 0xffff {
                    dst.put_u16_le(0xffff);
                } else {
                    dst.put_u64_le(0xffffffffffffffff)
                }
            }
            (ColumnData::String(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::NVarchar || vlc.r#type() == VarLenType::NChar =>
            {
                if let Some(str) = opt {
                    if vlc.len() < 0xffff {
                        let len_pos = dst.len();
                        dst.put_u16_le(0u16);

                        for chr in str.encode_utf16() {
                            dst.put_u16_le(chr);
                        }

                        let length = dst.len() - len_pos - 2;

                        if length > vlc.len() {
                            return Err(crate::mssql::connection::Error::BulkInput(
                                format!(
                                    "Encoded string length {} exceed column limit {}",
                                    length,
                                    vlc.len()
                                )
                                .into(),
                            ));
                        }

                        let dst: &mut [u8] = dst.borrow_mut();
                        let mut dst = &mut dst[len_pos..];
                        dst.put_u16_le(length as u16);
                    } else {
                        // unknown size
                        dst.put_u64_le(0xfffffffffffffffe);

                        assert!(
                            str.len() < 0xffffffff,
                            "if str longer than this, need to implement multiple blobs"
                        );

                        let len_pos = dst.len();
                        dst.put_u32_le(0u32);

                        for chr in str.encode_utf16() {
                            dst.put_u16_le(chr);
                        }

                        let length = dst.len() - len_pos - 4;

                        if length > vlc.len() {
                            return Err(crate::mssql::connection::Error::BulkInput(
                                format!(
                                    "Encoded string length {} exceed column limit {}",
                                    length,
                                    vlc.len()
                                )
                                .into(),
                            ));
                        }

                        if length > 0 {
                            // no next blob
                            dst.put_u32_le(0u32);
                        }

                        let dst: &mut [u8] = dst.borrow_mut();
                        let mut dst = &mut dst[len_pos..];
                        dst.put_u32_le(length as u32);
                    }
                } else if vlc.len() < 0xffff {
                    dst.put_u16_le(0xffff);
                } else {
                    dst.put_u64_le(0xffffffffffffffff)
                }
            }
            (ColumnData::String(Some(ref s)), None) if s.len() <= 4000 => {
                dst.put_u8(VarLenType::NVarchar as u8);
                dst.put_u16_le(8000);
                dst.extend_from_slice(&[0u8; 5][..]);

                let mut length = 0u16;
                let len_pos = dst.len();

                dst.put_u16_le(length);

                for chr in s.encode_utf16() {
                    length += 1;
                    dst.put_u16_le(chr);
                }

                let dst: &mut [u8] = dst.borrow_mut();
                let bytes = (length * 2).to_le_bytes(); // u16, two bytes

                for (i, byte) in bytes.iter().enumerate() {
                    dst[len_pos + i] = *byte;
                }
            }
            (ColumnData::String(Some(ref s)), None) => {
                // length: 0xffff and raw collation
                dst.put_u8(VarLenType::NVarchar as u8);
                dst.extend_from_slice(&[0xff_u8; 2]);
                dst.extend_from_slice(&[0u8; 5]);

                // we cannot cheaply predetermine the length of the UCS2 string beforehand
                // (2 * bytes(UTF8) is not always right) - so just let the SQL server handle it
                dst.put_u64_le(0xfffffffffffffffe_u64);

                // Write the varchar length
                let mut length = 0u32;
                let len_pos = dst.len();

                dst.put_u32_le(length);

                for chr in s.encode_utf16() {
                    length += 1;
                    dst.put_u16_le(chr);
                }

                if length > 0 {
                    // PLP_TERMINATOR
                    dst.put_u32_le(0);
                }

                let dst: &mut [u8] = dst.borrow_mut();
                let bytes = (length * 2).to_le_bytes(); // u32, four bytes

                for (i, byte) in bytes.iter().enumerate() {
                    dst[len_pos + i] = *byte;
                }
            }
            (ColumnData::Binary(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::BigBinary
                    || vlc.r#type() == VarLenType::BigVarBin =>
            {
                if let Some(bytes) = opt {
                    if bytes.len() > vlc.len() {
                        return Err(crate::mssql::connection::Error::BulkInput(
                            format!(
                                "Binary length {} exceed column limit {}",
                                bytes.len(),
                                vlc.len()
                            )
                            .into(),
                        ));
                    }

                    if vlc.len() < 0xffff {
                        dst.put_u16_le(bytes.len() as u16);
                        dst.extend(bytes.into_owned());
                    } else {
                        // unknown size
                        dst.put_u64_le(0xfffffffffffffffe);
                        dst.put_u32_le(bytes.len() as u32);

                        if !bytes.is_empty() {
                            dst.extend(bytes.into_owned());
                            dst.put_u32_le(0);
                        }
                    }
                } else if vlc.len() < 0xffff {
                    dst.put_u16_le(0xffff);
                } else {
                    dst.put_u64_le(0xffffffffffffffff);
                }
            }
            (ColumnData::Binary(Some(bytes)), None) if bytes.len() <= 8000 => {
                dst.put_u8(VarLenType::BigVarBin as u8);
                dst.put_u16_le(8000);
                dst.put_u16_le(bytes.len() as u16);
                dst.extend(bytes.into_owned());
            }
            (ColumnData::Binary(Some(bytes)), None) => {
                dst.put_u8(VarLenType::BigVarBin as u8);
                // Max length
                dst.put_u16_le(0xffff_u16);
                // Also the length is unknown
                dst.put_u64_le(0xfffffffffffffffe_u64);
                // We'll write in one chunk, length is the whole bytes length
                dst.put_u32_le(bytes.len() as u32);

                if !bytes.is_empty() {
                    // Payload
                    dst.extend(bytes.into_owned());
                    // PLP_TERMINATOR
                    dst.put_u32_le(0);
                }
            }
            (ColumnData::DateTime(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Datetimen =>
            {
                if let Some(dt) = opt {
                    dst.put_u8(8);
                    dt.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::DateTime(Some(dt)), Some(TypeInfo::FixedLen(FixedLenType::Datetime))) => {
                dt.encode(dst)?;
            }
            (ColumnData::DateTime(Some(dt)), None) => {
                dst.extend_from_slice(&[VarLenType::Datetimen as u8, 8, 8]);
                dt.encode(&mut *dst)?;
            }
            (ColumnData::SmallDateTime(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Datetimen =>
            {
                if let Some(dt) = opt {
                    dst.put_u8(4);
                    dt.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            (
                ColumnData::SmallDateTime(Some(dt)),
                Some(TypeInfo::FixedLen(FixedLenType::Datetime4)),
            ) => {
                dt.encode(dst)?;
            }
            (ColumnData::SmallDateTime(Some(dt)), None) => {
                dst.extend_from_slice(&[VarLenType::Datetimen as u8, 4, 4]);
                dt.encode(&mut *dst)?;
            }
            (ColumnData::Date(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Daten =>
            {
                if let Some(dt) = opt {
                    dst.put_u8(3);
                    dt.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::Date(Some(date)), None) => {
                dst.extend_from_slice(&[VarLenType::Daten as u8, 3]);
                date.encode(&mut *dst)?;
            }
            (ColumnData::Time(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Timen =>
            {
                if let Some(time) = opt {
                    dst.put_u8(time.len()?);
                    time.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::Time(Some(time)), None) => {
                dst.extend_from_slice(&[VarLenType::Timen as u8, time.scale(), time.len()?]);
                time.encode(&mut *dst)?;
            }
            (ColumnData::DateTime2(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::Datetime2 =>
            {
                if let Some(mut dt2) = opt {
                    if dt2.time().scale() != vlc.len() as u8 {
                        let time = dt2.time();
                        let increments = (time.increments() as f64
                            * 10_f64.powi(vlc.len() as i32 - time.scale() as i32))
                            as u64;
                        dt2 = DateTime2::new(dt2.date(), Time::new(increments, vlc.len() as u8));
                    }
                    dst.put_u8(dt2.time().len()? + 3);
                    dt2.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::DateTime2(Some(dt)), None) => {
                let len = dt.time().len()? + 3;
                dst.extend_from_slice(&[VarLenType::Datetime2 as u8, dt.time().scale(), len]);
                dt.encode(&mut *dst)?;
            }
            (ColumnData::DateTimeOffset(opt), Some(TypeInfo::VarLenSized(vlc)))
                if vlc.r#type() == VarLenType::DatetimeOffsetn =>
            {
                if let Some(dto) = opt {
                    dst.put_u8(dto.datetime2().time().len()? + 5);
                    dto.encode(dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::DateTimeOffset(Some(dto)), None) => {
                let headers = [
                    VarLenType::DatetimeOffsetn as u8,
                    dto.datetime2().time().scale(),
                    dto.datetime2().time().len()? + 5,
                ];

                dst.extend_from_slice(&headers);
                dto.encode(&mut *dst)?;
            }
            (ColumnData::Xml(opt), Some(TypeInfo::Xml { .. })) => {
                if let Some(xml) = opt {
                    xml.into_owned().encode(dst)?;
                } else {
                    dst.put_u64_le(0xffffffffffffffff_u64);
                }
            }
            (ColumnData::Xml(Some(xml)), None) => {
                dst.put_u8(VarLenType::Xml as u8);
                dst.put_u8(0);
                xml.into_owned().encode(&mut *dst)?;
            }
            (ColumnData::Numeric(opt), Some(TypeInfo::VarLenSizedPrecision { ty, scale, .. }))
                if ty == &VarLenType::Numericn || ty == &VarLenType::Decimaln =>
            {
                if let Some(num) = opt {
                    if scale != &num.scale() {
                        todo!("this still need some work, if client scale not aligned with server, we need to do conversion but will lose precision")
                    }
                    num.encode(&mut *dst)?;
                } else {
                    dst.put_u8(0);
                }
            }
            (ColumnData::Numeric(Some(num)), None) => {
                let headers = &[
                    VarLenType::Numericn as u8,
                    num.len(),
                    num.precision(),
                    num.scale(),
                ];

                dst.extend_from_slice(headers);
                num.encode(&mut *dst)?;
            }
            (_, None) => {
                // None/null
                dst.put_u8(FixedLenType::Null as u8);
            }
            (v, ref ti) => {
                return Err(crate::mssql::connection::Error::BulkInput(
                    format!("invalid data type, expecting {:?} but found {:?}", ti, v).into(),
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mssql::connection::sql_read_bytes::test_utils::IntoSqlReadBytes;
    use crate::mssql::connection::tds::Collation;
    use crate::mssql::connection::{Error, VarLenContext};
    use bytes::BytesMut;

    fn test_round_trip(ti: TypeInfo, d: ColumnData<'_>) {
        let mut buf = BytesMut::new();
        let mut buf_with_ti = BytesMutWithTypeInfo::new(&mut buf).with_type_info(&ti);

        d.clone()
            .encode(&mut buf_with_ti)
            .expect("encode must succeed");

        let reader = &mut buf.into_sql_read_bytes();
        let nd = ColumnData::decode(reader, &ti).expect("decode must succeed");

        assert_eq!(nd, d);

        reader
            .read_u8()
            .expect_err("decode must consume entire buffer");
    }

    fn i32_with_varlen_int() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Intn, 4, None)),
            ColumnData::I32(Some(42)),
        );
    }

    fn none_with_varlen_int() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Intn, 4, None)),
            ColumnData::I32(None),
        );
    }

    fn i32_with_fixedlen_int() {
        test_round_trip(
            TypeInfo::FixedLen(FixedLenType::Int4),
            ColumnData::I32(Some(42)),
        );
    }

    fn bit_with_varlen_bit() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Bitn, 1, None)),
            ColumnData::Bit(Some(true)),
        );
    }

    fn none_with_varlen_bit() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Bitn, 1, None)),
            ColumnData::Bit(None),
        );
    }

    fn bit_with_fixedlen_bit() {
        test_round_trip(
            TypeInfo::FixedLen(FixedLenType::Bit),
            ColumnData::Bit(Some(true)),
        );
    }

    fn u8_with_varlen_int() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Intn, 1, None)),
            ColumnData::U8(Some(8u8)),
        );
    }

    fn none_u8_with_varlen_int() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Intn, 1, None)),
            ColumnData::U8(None),
        );
    }

    fn u8_with_fixedlen_int() {
        test_round_trip(
            TypeInfo::FixedLen(FixedLenType::Int1),
            ColumnData::U8(Some(8u8)),
        );
    }

    fn i16_with_varlen_intn() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Intn, 2, None)),
            ColumnData::I16(Some(8i16)),
        );
    }

    fn none_i16_with_varlen_intn() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Intn, 2, None)),
            ColumnData::I16(None),
        );
    }

    fn none_with_varlen_intn() {
        test_round_trip(
            TypeInfo::FixedLen(FixedLenType::Int2),
            ColumnData::I16(Some(8i16)),
        );
    }

    fn i64_with_varlen_intn() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Intn, 8, None)),
            ColumnData::I64(Some(8i64)),
        );
    }

    fn i64_none_with_varlen_intn() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Intn, 8, None)),
            ColumnData::I64(None),
        );
    }

    fn i64_with_fixedlen_int8() {
        test_round_trip(
            TypeInfo::FixedLen(FixedLenType::Int8),
            ColumnData::I64(Some(8i64)),
        );
    }

    fn f32_with_varlen_floatn() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Floatn, 4, None)),
            ColumnData::F32(Some(8f32)),
        );
    }

    fn null_f32_with_varlen_floatn() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Floatn, 4, None)),
            ColumnData::F32(None),
        );
    }

    fn f32_with_fixedlen_float4() {
        test_round_trip(
            TypeInfo::FixedLen(FixedLenType::Float4),
            ColumnData::F32(Some(8f32)),
        );
    }

    fn f64_with_varlen_floatn() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Floatn, 8, None)),
            ColumnData::F64(Some(8f64)),
        );
    }

    fn none_f64_with_varlen_floatn() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Floatn, 8, None)),
            ColumnData::F64(None),
        );
    }

    fn f64_with_fixedlen_float8() {
        test_round_trip(
            TypeInfo::FixedLen(FixedLenType::Float8),
            ColumnData::F64(Some(8f64)),
        );
    }

    fn guid_with_varlen_guid() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Guid, 16, None)),
            ColumnData::Guid(Some(Uuid::new_v4())),
        );
    }

    fn none_guid_with_varlen_guid() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Guid, 16, None)),
            ColumnData::Guid(None),
        );
    }

    fn numeric_with_varlen_sized_precision() {
        test_round_trip(
            TypeInfo::VarLenSizedPrecision {
                ty: VarLenType::Numericn,
                size: 17,
                precision: 18,
                scale: 0,
            },
            ColumnData::Numeric(Some(Numeric::new_with_scale(23, 0))),
        );
    }

    fn none_numeric_with_varlen_sized_precision() {
        test_round_trip(
            TypeInfo::VarLenSizedPrecision {
                ty: VarLenType::Numericn,
                size: 17,
                precision: 18,
                scale: 0,
            },
            ColumnData::Numeric(None),
        );
    }

    fn string_with_varlen_bigchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::BigChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("aaa".into())),
        );
    }

    fn long_string_with_varlen_bigchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::BigChar,
                0x8ffff,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("aaa".into())),
        );
    }

    fn none_long_string_with_varlen_bigchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::BigChar,
                0x8ffff,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(None),
        );
    }

    fn none_string_with_varlen_bigchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::BigChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(None),
        );
    }

    fn string_with_varlen_bigvarchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::BigVarChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("aaa".into())),
        );
    }

    fn none_string_with_varlen_bigvarchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::BigVarChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(None),
        );
    }

    fn empty_string_with_varlen_bigvarchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::BigVarChar,
                0x8ffff,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("".into())),
        );
    }

    fn string_with_varlen_nvarchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::NVarchar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("hhh".into())),
        );
    }

    fn none_string_with_varlen_nvarchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::NVarchar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(None),
        );
    }

    fn empty_string_with_varlen_nvarchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::NVarchar,
                0x8ffff,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("".into())),
        );
    }

    fn string_with_varlen_nchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::NChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("hhh".into())),
        );
    }

    fn long_string_with_varlen_nchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::NChar,
                0x8ffff,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(Some("hhh".into())),
        );
    }

    fn none_long_string_with_varlen_nchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::NChar,
                0x8ffff,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(None),
        );
    }

    fn none_string_with_varlen_nchar() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::NChar,
                40,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::String(None),
        );
    }

    fn binary_with_varlen_bigbinary() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::BigBinary, 40, None)),
            ColumnData::Binary(Some(b"aaa".as_slice().into())),
        );
    }

    fn long_binary_with_varlen_bigbinary() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::BigBinary, 0x8ffff, None)),
            ColumnData::Binary(Some(b"aaa".as_slice().into())),
        );
    }

    fn none_binary_with_varlen_bigbinary() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::BigBinary, 40, None)),
            ColumnData::Binary(None),
        );
    }

    fn none_long_binary_with_varlen_bigbinary() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::BigBinary, 0x8ffff, None)),
            ColumnData::Binary(None),
        );
    }

    fn binary_with_varlen_bigvarbin() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::BigVarBin, 40, None)),
            ColumnData::Binary(Some(b"aaa".as_slice().into())),
        );
    }

    fn none_binary_with_varlen_bigvarbin() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::BigVarBin, 40, None)),
            ColumnData::Binary(None),
        );
    }

    fn empty_binary_with_varlen_bigvarbin() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(
                VarLenType::BigVarBin,
                0x8ffff,
                Some(Collation::new(13632521, 52)),
            )),
            ColumnData::Binary(Some(b"".as_slice().into())),
        );
    }

    fn datetime_with_varlen_datetimen() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Datetimen, 8, None)),
            ColumnData::DateTime(Some(DateTime::new(200, 3000))),
        );
    }

    // this is inconsistent: decode will decode any None datetime to smalldatetime, ignoring size
    // but it's non-critical, so let it be here

    fn none_datetime_with_varlen_datetimen() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Datetimen, 8, None)),
            ColumnData::DateTime(None),
        );
    }

    fn datetime_with_fixedlen_datetime() {
        test_round_trip(
            TypeInfo::FixedLen(FixedLenType::Datetime),
            ColumnData::DateTime(Some(DateTime::new(200, 3000))),
        );
    }

    fn smalldatetime_with_varlen_datetimen() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Datetimen, 4, None)),
            ColumnData::SmallDateTime(Some(SmallDateTime::new(200, 3000))),
        );
    }

    fn none_smalldatetime_with_varlen_datetimen() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Datetimen, 4, None)),
            ColumnData::SmallDateTime(None),
        );
    }

    fn smalldatetime_with_fixedlen_datetime4() {
        test_round_trip(
            TypeInfo::FixedLen(FixedLenType::Datetime4),
            ColumnData::SmallDateTime(Some(SmallDateTime::new(200, 3000))),
        );
    }

    #[cfg(feature = "tds73")]

    fn date_with_varlen_daten() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Daten, 3, None)),
            ColumnData::Date(Some(Date::new(200))),
        );
    }

    #[cfg(feature = "tds73")]

    fn none_date_with_varlen_daten() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Daten, 3, None)),
            ColumnData::Date(None),
        );
    }

    #[cfg(feature = "tds73")]

    fn time_with_varlen_timen() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Timen, 7, None)),
            ColumnData::Time(Some(Time::new(55, 7))),
        );
    }

    #[cfg(feature = "tds73")]

    fn none_time_with_varlen_timen() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Timen, 7, None)),
            ColumnData::Time(None),
        );
    }

    #[cfg(feature = "tds73")]

    fn datetime2_with_varlen_datetime2() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Datetime2, 7, None)),
            ColumnData::DateTime2(Some(DateTime2::new(Date::new(55), Time::new(222, 7)))),
        );
    }

    #[cfg(feature = "tds73")]

    fn none_datetime2_with_varlen_datetime2() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Datetime2, 7, None)),
            ColumnData::DateTime2(None),
        );
    }

    #[cfg(feature = "tds73")]

    fn datetimeoffset_with_varlen_datetimeoffsetn() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::DatetimeOffsetn, 7, None)),
            ColumnData::DateTimeOffset(Some(DateTimeOffset::new(
                DateTime2::new(Date::new(55), Time::new(222, 7)),
                -8,
            ))),
        );
    }

    #[cfg(feature = "tds73")]

    fn none_datetimeoffset_with_varlen_datetimeoffsetn() {
        test_round_trip(
            TypeInfo::VarLenSized(VarLenContext::new(VarLenType::DatetimeOffsetn, 7, None)),
            ColumnData::DateTimeOffset(None),
        );
    }

    #[cfg(feature = "tds73")]

    fn xml_with_xml() {
        test_round_trip(
            TypeInfo::Xml {
                schema: None,
                size: 0xfffffffffffffffe_usize,
            },
            ColumnData::Xml(Some(Cow::Owned(XmlData::new("<a>ddd</a>")))),
        );
    }

    #[cfg(feature = "tds73")]

    fn none_xml_with_xml() {
        test_round_trip(
            TypeInfo::Xml {
                schema: None,
                size: 0xfffffffffffffffe_usize,
            },
            ColumnData::Xml(None),
        );
    }

    fn invalid_type_fails() {
        let data = vec![
            (
                TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Floatn, 4, None)),
                ColumnData::I32(Some(42)),
            ),
            (
                TypeInfo::VarLenSized(VarLenContext::new(VarLenType::Floatn, 4, None)),
                ColumnData::I32(None),
            ),
            (
                TypeInfo::FixedLen(FixedLenType::Int4),
                ColumnData::I32(None),
            ),
        ];

        for (ti, d) in data {
            let mut buf = BytesMut::new();
            let mut buf_ti = BytesMutWithTypeInfo::new(&mut buf).with_type_info(&ti);

            let err = d.encode(&mut buf_ti).expect_err("encode should fail");

            if let Error::BulkInput(_) = err {
            } else {
                panic!("Expected: Error::BulkInput, got: {:?}", err);
            }
        }
    }
}
