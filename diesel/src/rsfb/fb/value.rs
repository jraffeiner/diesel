//! Firebird value representation

use super::backend::Fb;
use crate::row::RowSealed;
use crate::row::{Field, PartialRow, Row as DsRow, RowIndex};
pub use rsfbclient::Column;
use rsfbclient::Row as RsRow;
use std::ops::Range;

#[derive(Debug)]
/// Represent a value in a row
pub struct FbValue<'a> {
    /// The Raw Column
    pub raw: &'a Column,
}

#[derive(Debug)]
/// Represents a Field of a Row
pub struct FbField<'a> {
    raw: &'a Column,
}

impl<'a> Field<'a, Fb> for FbField<'a> {
    fn field_name(&self) -> Option<&'a str> {
        Some(self.raw.name.as_str())
    }

    fn value(&self) -> Option<<Fb as crate::backend::Backend>::RawValue<'_>> {
        if self.raw.value.is_null() {
            return None;
        }

        Some(FbValue { raw: self.raw })
    }
}

#[expect(missing_debug_implementations)]
/// Represents a Row in a table
pub struct FbRow {
    raw: RsRow,
}

impl FbRow {
    /// Creates a row from a Fb-Row
    pub fn new(row: RsRow) -> Self {
        Self { raw: row }
    }
}
impl RowSealed for FbRow {}

impl<'a> DsRow<'a, Fb> for FbRow {
    type Field<'f>
        = FbField<'f>
    where
        'a: 'f,
        Self: 'f;

    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.raw.cols.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'a: 'b,
        Self: RowIndex<I>,
    {
        let idx = self.idx(idx)?;
        if let Some(col) = self.raw.cols.get(idx) {
            return Some(Self::Field { raw: col });
        }

        None
    }

    fn partial_row(&self, range: Range<usize>) -> PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

impl RowIndex<usize> for FbRow {
    fn idx(&self, idx: usize) -> Option<usize> {
        if idx < self.raw.cols.len() {
            Some(idx)
        } else {
            None
        }
    }
}

impl<'a> RowIndex<&'a str> for FbRow {
    fn idx(&self, field_name: &'a str) -> Option<usize> {
        self.raw
            .cols
            .iter()
            .position(|col| col.name.to_lowercase() == field_name.to_lowercase())
    }
}
