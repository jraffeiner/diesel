//! Mappings between TDS and and Chrono types (with `chrono` feature flag
//! enabled).
//!
//! The chrono library offers better ergonomy, but is known to hold certain
//! security vulnerabilities. The code here is for legacy purposes, please use
//! `time` crate for greenfield projects.

use jiff::tz::{Offset, TimeZone};
use jiff::{Span, Timestamp, Zoned};
use jiff::civil::{Date as CivilDate, DateTime as CivilDateTime, Time as CivilTime};

use super::{Date, DateTime2, DateTimeOffset, Time};
use crate::mssql::connection::tds::codec::ColumnData;
#[inline]
fn from_days(days: i64, start_year: i16) -> CivilDate {
    CivilDate::new(start_year, 1, 1).unwrap() + Span::new().days(days)
}

#[inline]
fn from_sec_fragments(sec_fragments: i64) -> CivilTime {
    CivilTime::midnight() + Span::new().nanoseconds(sec_fragments * (1e9 as i64) / 300)
}

#[inline]
fn from_date(date: Date) -> CivilDate {
    from_days(date.days() as i64, 1)
}

#[inline]
fn from_time(time: Time) -> CivilTime {
    CivilTime::midnight() + Span::new().nanoseconds(time.increments as i64 * 10i64.pow(9 - time.scale as u32))
}

#[inline]
fn to_date(date: CivilDate) -> Date {
    Date::new((date - CivilDate::new(1, 1, 1).unwrap()).get_days() as u32)
}

#[inline]
fn to_time(time: CivilTime) -> Time {
    let increments = (time.duration_since(CivilTime::midnight()).as_nanos() / 100) as u64;
    Time { increments, scale: 7 }
}

#[inline]
#[expect(unused)]
fn to_sec_fragments(time: CivilTime) -> i64 {
    (time.duration_since(CivilTime::midnight())
        .as_nanos()
        * 300
        / (1e9 as i128)) as i64
}


from_sql!(
    CivilDateTime:
        ColumnData::SmallDateTime(ref dt) => dt.map(|dt| CivilDateTime::from_parts(
            from_days(dt.days as i64, 1900), 
            from_sec_fragments(dt.seconds_fragments as i64)
        )),
        ColumnData::DateTime2(ref dt) => dt.map(|dt| CivilDateTime::from_parts(
            from_date(dt.date), 
            from_time(dt.time),
        )),
        ColumnData::DateTime(ref dt) => dt.map(|dt| CivilDateTime::from_parts(
            from_days(dt.days as i64, 1900), 
            from_sec_fragments(dt.seconds_fragments as i64)
        ));
    CivilTime:
        ColumnData::Time(ref time) => time.map(|time| from_time(time));
    CivilDate:
        ColumnData::Date(ref date) => date.map(|date| from_date(date));
    Zoned:
        ColumnData::DateTimeOffset(ref dto) => dto.and_then(|dto| {
            let tz = TimeZone::fixed(Offset::from_seconds((dto.offset()*60).into()).unwrap());
            CivilDateTime::from_parts(
                from_date(dto.datetime2.date), 
                from_time(dto.datetime2.time),
            ).to_zoned(TimeZone::UTC).map(|zoned| zoned.with_time_zone(tz)).ok()
        }),
        ColumnData::DateTime2(ref dt2) => dt2.and_then(|dt2| {
            CivilDateTime::from_parts(
                from_date(dt2.date), 
                from_time(dt2.time),
            ).to_zoned(TimeZone::UTC).ok()
        });
    Timestamp:
        ColumnData::DateTimeOffset(ref dto) => dto.and_then(|dto| {
            CivilDateTime::from_parts(
                from_date(dto.datetime2.date), 
                from_time(dto.datetime2.time),
            ).to_zoned(TimeZone::UTC).map(|zoned| zoned.timestamp()).ok()
        }),
        ColumnData::DateTime2(ref dt2) => dt2.and_then(|dt2| {
            CivilDateTime::from_parts(
                from_date(dt2.date), 
                from_time(dt2.time),
            ).to_zoned(TimeZone::UTC).map(|zoned| zoned.timestamp()).ok()
        })
);

to_sql!(self_,
        CivilDate: (ColumnData::Date, to_date(*self_));
        CivilTime: (ColumnData::Time, to_time(*self_));
        CivilDateTime: (ColumnData::DateTime2, {
            let date = to_date(self_.date());
            let time = to_time(self_.time());

            DateTime2::new(date, time)
        });
        Zoned: (ColumnData::DateTimeOffset, {
            let offset = (self_.offset().seconds() / 60) as i16;
            let utc = self_.with_time_zone(TimeZone::UTC);

            let date = to_date(utc.date());
            let time = to_time(utc.time());

            DateTimeOffset::new(DateTime2::new(date, time), offset)
        });
        Timestamp: (ColumnData::DateTime2, {
            let zoned = self_.to_zoned(TimeZone::UTC);

            let date = to_date(zoned.date());
            let time = to_time(zoned.time());

            DateTime2::new(date, time)
        });
);

into_sql!(self_,
        CivilDate: (ColumnData::Date, to_date(self_));
        CivilTime: (ColumnData::Time, to_time(self_));
        CivilDateTime: (ColumnData::DateTime2, {
            let date = to_date(self_.date());
            let time = to_time(self_.time());

            DateTime2::new(date, time)
        });
        Zoned: (ColumnData::DateTimeOffset, {
            let offset = (self_.offset().seconds() / 60) as i16;
            let utc = self_.with_time_zone(TimeZone::UTC);

            let date = to_date(utc.date());
            let time = to_time(utc.time());

            DateTimeOffset::new(DateTime2::new(date, time), offset)
        });
        Timestamp: (ColumnData::DateTime2, {
            let zoned = self_.to_zoned(TimeZone::UTC);

            let date = to_date(zoned.date());
            let time = to_time(zoned.time());

            DateTime2::new(date, time)
        });
);
