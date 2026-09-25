//! # Oracle Arrow Conversion
//!
//! Provides utilities to convert Oracle result set rows into Arrow record batches.

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    Float32Builder, Float64Builder, Int32Builder, Int64Builder, IntervalMonthDayNanoBuilder,
    IntervalYearMonthBuilder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder,
};
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, TimeZone, Utc};
use std::sync::Arc;
use thiserror::Error;

use crate::sources::sql::oracle::conn::map_oracle_type_to_arrow;

/// Errors that can occur during Oracle to Arrow conversion.
#[derive(Debug, Error)]
pub enum ArrowError {
    /// An error occurred in the underlying Oracle driver.
    #[error("Oracle error: {0}")]
    OracleError(#[from] rust_oracle::Error),

    /// A column type could not be mapped to an Arrow data type.
    #[error("Failed to map column {0} to arrow type")]
    FailedToMapColumnType(String),

    /// A builder could not be downcast to the expected concrete type.
    #[error("Failed to downcast builder at index {0}")]
    FailedToDowncastBuilder(usize),

    /// An error occurred in the Arrow library.
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
}

/// A specialized `Result` type for Oracle to Arrow conversion operations.
pub type Result<T> = std::result::Result<T, ArrowError>;

enum ColumnExtractor {
    Boolean(BooleanBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Binary(BinaryBuilder),
    LargeBinary(LargeBinaryBuilder),
    Date32(Date32Builder),
    TimestampSecond(TimestampSecondBuilder),
    TimestampMillisecond(TimestampMillisecondBuilder),
    TimestampMicrosecond(TimestampMicrosecondBuilder),
    TimestampNanosecond(TimestampNanosecondBuilder),
    IntervalYearMonth(IntervalYearMonthBuilder),
    IntervalMonthDayNano(IntervalMonthDayNanoBuilder),
    Decimal128(Decimal128Builder, i8),
    Decimal256(Decimal256Builder, i8),
    Fallback(StringBuilder),
}

impl ColumnExtractor {
    /// Builds an extractor for `dt`.
    ///
    /// # Errors
    ///
    /// Returns an error if `dt` is a decimal whose precision/scale pair Arrow
    /// rejects (e.g. `precision == 0`). Callers must normalize metadata-derived
    /// precision/scale before reaching this point; substituting a default here
    /// would produce an array whose type silently disagrees with the projected
    /// schema, which `RecordBatch::try_new` reports only as an opaque mismatch.
    fn new(dt: &DataType, capacity: usize) -> Result<Self> {
        Ok(match dt {
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
            DataType::LargeUtf8 => {
                Self::LargeUtf8(LargeStringBuilder::with_capacity(capacity, capacity * 16))
            }
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(capacity, capacity * 16)),
            DataType::LargeBinary => {
                Self::LargeBinary(LargeBinaryBuilder::with_capacity(capacity, capacity * 16))
            }
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Decimal128(p, s) => Self::Decimal128(
                Decimal128Builder::with_capacity(capacity).with_precision_and_scale(*p, *s)?,
                *s,
            ),
            DataType::Decimal256(p, s) => Self::Decimal256(
                Decimal256Builder::with_capacity(capacity).with_precision_and_scale(*p, *s)?,
                *s,
            ),
            DataType::Timestamp(TimeUnit::Second, tz) => Self::TimestampSecond(
                TimestampSecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone()),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, tz) => Self::TimestampMillisecond(
                TimestampMillisecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone()),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => Self::TimestampMicrosecond(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone()),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => Self::TimestampNanosecond(
                TimestampNanosecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone()),
            ),
            DataType::Interval(IntervalUnit::YearMonth) => {
                Self::IntervalYearMonth(IntervalYearMonthBuilder::with_capacity(capacity))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                Self::IntervalMonthDayNano(IntervalMonthDayNanoBuilder::with_capacity(capacity))
            }
            _ => Self::Fallback(StringBuilder::with_capacity(capacity, capacity * 16)),
        })
    }

    fn append_row(&mut self, row: &rust_oracle::Row, i: usize) -> Result<()> {
        match self {
            Self::Boolean(b) => {
                let v: Option<bool> = row.get(i)?;
                b.append_option(v);
            }
            Self::Int32(b) => {
                let v: Option<i32> = row.get(i)?;
                b.append_option(v);
            }
            Self::Int64(b) => {
                let v: Option<i64> = row.get(i)?;
                b.append_option(v);
            }
            Self::Float32(b) => {
                let v: Option<f32> = row.get(i)?;
                b.append_option(v);
            }
            Self::Float64(b) => {
                let v: Option<f64> = row.get(i)?;
                b.append_option(v);
            }
            Self::Utf8(b) => {
                let v: Option<String> = row.get(i)?;
                b.append_option(v);
            }
            Self::LargeUtf8(b) => {
                let v: Option<String> = row.get(i)?;
                b.append_option(v);
            }
            Self::Date32(b) => {
                let v: Option<rust_oracle::sql_type::Timestamp> = row.get(i)?;
                if let Some(ts) = v {
                    if let Some(date) = NaiveDate::from_ymd_opt(ts.year(), ts.month(), ts.day()) {
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or(date);
                        let days = date.signed_duration_since(epoch).num_days() as i32;
                        b.append_value(days);
                    } else {
                        b.append_null();
                    }
                } else {
                    b.append_null();
                }
            }
            Self::TimestampSecond(b) => {
                let v: Option<rust_oracle::sql_type::Timestamp> = row.get(i)?;
                if let Some(ts) = v {
                    let chrono_ts = Utc
                        .with_ymd_and_hms(
                            ts.year(),
                            ts.month(),
                            ts.day(),
                            ts.hour(),
                            ts.minute(),
                            ts.second(),
                        )
                        .single();
                    if let Some(c) = chrono_ts {
                        b.append_value(c.timestamp());
                    } else {
                        b.append_null();
                    }
                } else {
                    b.append_null();
                }
            }
            Self::TimestampMillisecond(b) => {
                let v: Option<rust_oracle::sql_type::Timestamp> = row.get(i)?;
                if let Some(ts) = v {
                    let chrono_ts = Utc
                        .with_ymd_and_hms(
                            ts.year(),
                            ts.month(),
                            ts.day(),
                            ts.hour(),
                            ts.minute(),
                            ts.second(),
                        )
                        .single();
                    if let Some(c) = chrono_ts {
                        b.append_value(c.timestamp_millis());
                    } else {
                        b.append_null();
                    }
                } else {
                    b.append_null();
                }
            }
            Self::TimestampMicrosecond(b) => {
                let v: Option<rust_oracle::sql_type::Timestamp> = row.get(i)?;
                if let Some(ts) = v {
                    let chrono_ts = Utc
                        .with_ymd_and_hms(
                            ts.year(),
                            ts.month(),
                            ts.day(),
                            ts.hour(),
                            ts.minute(),
                            ts.second(),
                        )
                        .single();
                    if let Some(c) = chrono_ts {
                        b.append_value(c.timestamp_micros());
                    } else {
                        b.append_null();
                    }
                } else {
                    b.append_null();
                }
            }
            Self::TimestampNanosecond(b) => {
                let v: Option<rust_oracle::sql_type::Timestamp> = row.get(i)?;
                if let Some(ts) = v {
                    let chrono_ts = Utc
                        .with_ymd_and_hms(
                            ts.year(),
                            ts.month(),
                            ts.day(),
                            ts.hour(),
                            ts.minute(),
                            ts.second(),
                        )
                        .single();
                    if let Some(c) = chrono_ts {
                        b.append_value(c.timestamp_nanos_opt().unwrap_or(0));
                    } else {
                        b.append_null();
                    }
                } else {
                    b.append_null();
                }
            }
            Self::Decimal128(b, s) => {
                let v: Option<String> = row.get(i)?;
                if let Some(val_str) = v {
                    if let Some(scaled) = parse_decimal::<i128>(&val_str, *s) {
                        b.append_value(scaled);
                    } else {
                        b.append_null();
                    }
                } else {
                    b.append_null();
                }
            }
            Self::Decimal256(b, s) => {
                let v: Option<String> = row.get(i)?;
                if let Some(val_str) = v {
                    // Must apply the declared scale. Storing the raw digits
                    // (e.g. "1234" for 12.34 at scale 2) would read back as
                    // 1234.00 because the array is declared with scale = s.
                    if let Some(scaled) = parse_decimal::<arrow::datatypes::i256>(&val_str, *s) {
                        b.append_value(scaled);
                    } else {
                        b.append_null();
                    }
                } else {
                    b.append_null();
                }
            }
            Self::Binary(b) => {
                let v: Option<Vec<u8>> = row.get(i)?;
                b.append_option(v);
            }
            Self::LargeBinary(b) => {
                let v: Option<Vec<u8>> = row.get(i)?;
                b.append_option(v);
            }
            Self::IntervalYearMonth(b) => {
                let v: Option<rust_oracle::sql_type::IntervalYM> = row.get(i)?;
                if let Some(ym) = v {
                    // Oracle permits 9-digit INTERVAL YEAR TO MONTH precision, so
                    // `years() * 12` can exceed i32. Saturate rather than wrap
                    // (which would yield a negative month count).
                    b.append_value(ym.years().saturating_mul(12).saturating_add(ym.months()));
                } else {
                    b.append_null();
                }
            }
            Self::IntervalMonthDayNano(b) => {
                let v: Option<rust_oracle::sql_type::IntervalDS> = row.get(i)?;
                if let Some(ds) = v {
                    let total_nanos =
                        (ds.hours() as i64 * 3600 + ds.minutes() as i64 * 60 + ds.seconds() as i64)
                            * 1_000_000_000
                            + ds.nanoseconds() as i64;
                    b.append_value(arrow::datatypes::IntervalMonthDayNanoType::make_value(
                        0,
                        ds.days(),
                        total_nanos,
                    ));
                } else {
                    b.append_null();
                }
            }
            Self::Fallback(b) => {
                let v: Option<String> = row.get(i).ok().flatten();
                b.append_option(v);
            }
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Boolean(mut b) => Arc::new(b.finish()),
            Self::Int32(mut b) => Arc::new(b.finish()),
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::Float32(mut b) => Arc::new(b.finish()),
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::LargeUtf8(mut b) => Arc::new(b.finish()),
            Self::Binary(mut b) => Arc::new(b.finish()),
            Self::LargeBinary(mut b) => Arc::new(b.finish()),
            Self::Date32(mut b) => Arc::new(b.finish()),
            Self::TimestampSecond(mut b) => Arc::new(b.finish()),
            Self::TimestampMillisecond(mut b) => Arc::new(b.finish()),
            Self::TimestampMicrosecond(mut b) => Arc::new(b.finish()),
            Self::TimestampNanosecond(mut b) => Arc::new(b.finish()),
            Self::IntervalYearMonth(mut b) => Arc::new(b.finish()),
            Self::IntervalMonthDayNano(mut b) => Arc::new(b.finish()),
            Self::Decimal128(mut b, _) => Arc::new(b.finish()),
            Self::Decimal256(mut b, _) => Arc::new(b.finish()),
            Self::Fallback(mut b) => Arc::new(b.finish()),
        }
    }
}

/// Converts a vector of Oracle rows into an Arrow `RecordBatch`.
pub fn rows_to_arrow(
    rows: Vec<rust_oracle::Row>,
    projected_schema: &Option<SchemaRef>,
) -> Result<RecordBatch> {
    if rows.is_empty() {
        let schema = projected_schema
            .clone()
            .unwrap_or_else(|| Arc::new(Schema::empty()));
        return Ok(RecordBatch::new_empty(schema));
    }

    let first_row = &rows[0];
    let mut arrow_fields = Vec::new();
    let mut extractors: Vec<ColumnExtractor> = Vec::new();

    if let Some(schema) = projected_schema {
        for field in schema.fields() {
            arrow_fields.push(field.clone());
            extractors.push(ColumnExtractor::new(field.data_type(), rows.len())?);
        }
    } else {
        let column_info = first_row.column_info();
        for info in column_info {
            let name = info.name().to_string();
            let oracle_type_str = info.oracle_type().to_string();
            let data_type = map_oracle_type_to_arrow(&oracle_type_str, None, None);

            arrow_fields.push(Arc::new(Field::new(name, data_type.clone(), true)));
            extractors.push(ColumnExtractor::new(&data_type, rows.len())?);
        }
    }

    for row in &rows {
        for (i, extractor) in extractors.iter_mut().enumerate() {
            extractor.append_row(row, i)?;
        }
    }

    let columns: Vec<ArrayRef> = extractors.into_iter().map(|e| e.finish()).collect();
    let schema = Arc::new(Schema::new(arrow_fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Integer types usable as the unscaled backing store of an Arrow decimal array.
trait UnscaledDecimal: Copy + std::str::FromStr + std::ops::Add<Self, Output = Self> {
    const ONE: Self;
    fn negate(self) -> Self;
}

impl UnscaledDecimal for i128 {
    const ONE: Self = 1;
    fn negate(self) -> Self {
        -self
    }
}

impl UnscaledDecimal for arrow::datatypes::i256 {
    const ONE: Self = arrow::datatypes::i256::ONE;
    fn negate(self) -> Self {
        -self
    }
}

/// Parses a decimal string into the unscaled integer representation for a
/// decimal type `P` at the given `scale`.
///
/// Returns `None` if the string is not a plain decimal, or if `scale` is
/// negative. A negative `scale` must be rejected rather than cast: `scale as
/// usize` on a negative `i8` yields ~1.8e19, which would leave the fractional
/// digits in place and write an unscaled result into a negative-scale array
/// (Oracle permits exponent notation such as `NUMBER(5,-3)`). `map_oracle_type_to_arrow`
/// routes such columns to `Float64` before they reach here.
fn parse_decimal<P: UnscaledDecimal>(s: &str, scale: i8) -> Option<P> {
    let scale_usize = usize::try_from(scale).ok()?;

    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let mut is_negative = false;
    let mut chars = s.chars().peekable();
    if let Some(&c) = chars.peek() {
        if c == '-' {
            is_negative = true;
            chars.next();
        } else if c == '+' {
            chars.next();
        }
    }

    let mut integer_part = String::new();
    let mut fractional_part = String::new();
    let mut in_fraction = false;

    for c in chars {
        if c == '.' {
            in_fraction = true;
        } else if c.is_ascii_digit() {
            if in_fraction {
                fractional_part.push(c);
            } else {
                integer_part.push(c);
            }
        } else {
            return None;
        }
    }

    if integer_part.is_empty() {
        integer_part.push('0');
    }

    if fractional_part.len() > scale_usize {
        let round_digit = fractional_part.as_bytes()[scale_usize] - b'0';
        fractional_part.truncate(scale_usize);

        if round_digit >= 5 {
            let full_str = format!("{}{}", integer_part, fractional_part);
            if let Ok(val) = P::from_str(&full_str) {
                let rounded = val + P::ONE;
                return Some(if is_negative {
                    rounded.negate()
                } else {
                    rounded
                });
            }
        }
    } else {
        while fractional_part.len() < scale_usize {
            fractional_part.push('0');
        }
    }

    let full_str = format!("{}{}", integer_part, fractional_part);
    let val = P::from_str(&full_str).ok()?;

    Some(if is_negative { val.negate() } else { val })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::i256;

    #[test]
    fn parse_decimal_applies_scale() {
        // 12.34 at scale 4 must be stored as 123400, not 1234.
        let v = parse_decimal::<i128>("12.34", 4).unwrap();
        assert_eq!(v, 123_400);
        let v = parse_decimal::<i128>("12.34", 2).unwrap();
        assert_eq!(v, 1234);
        let v = parse_decimal::<i128>("12.34", 0).unwrap();
        assert_eq!(v, 12);
    }

    #[test]
    fn parse_decimal256_applies_scale() {
        // Regression: the Decimal256 arm used to drop the scale entirely and
        // store the raw digits, a 10^scale error.
        let v = parse_decimal::<i256>("12.34", 4).unwrap();
        assert_eq!(v, i256::from(123_400i64));
        let v = parse_decimal::<i256>("12.34", 2).unwrap();
        assert_eq!(v, i256::from(1234i64));
    }

    #[test]
    fn parse_decimal_handles_sign_and_padding() {
        assert_eq!(parse_decimal::<i128>("-0.5", 2).unwrap(), -50);
        assert_eq!(parse_decimal::<i128>("+7", 0).unwrap(), 7);
        assert_eq!(parse_decimal::<i128>(".25", 2).unwrap(), 25);
        assert_eq!(parse_decimal::<i128>("3", 3).unwrap(), 3000);
    }

    #[test]
    fn parse_decimal_rounds_half_away_from_zero() {
        assert_eq!(parse_decimal::<i128>("1.005", 2).unwrap(), 101);
        assert_eq!(parse_decimal::<i128>("-1.005", 2).unwrap(), -101);
        assert_eq!(parse_decimal::<i128>("1.004", 2).unwrap(), 100);
    }

    #[test]
    fn parse_decimal_rejects_negative_scale() {
        // `scale as usize` on a negative i8 would yield ~1.8e19 and silently
        // keep the fractional digits.
        assert_eq!(parse_decimal::<i128>("1000", -3), None);
        assert_eq!(parse_decimal::<i256>("1000", -3), None);
    }

    #[test]
    fn parse_decimal_rejects_non_decimal() {
        assert_eq!(parse_decimal::<i128>("1e5", 2), None);
        assert_eq!(parse_decimal::<i128>("abc", 2), None);
        assert_eq!(parse_decimal::<i128>("", 2), None);
    }

    #[test]
    fn decimal_extractor_rejects_zero_precision_instead_of_defaulting() {
        // Arrow rejects precision == 0; the builder must surface that rather
        // than silently becoming Decimal128(38, 10) and failing later with an
        // opaque RecordBatch type mismatch.
        let err = match ColumnExtractor::new(&DataType::Decimal128(0, 2), 16) {
            Ok(_) => panic!("precision 0 must be rejected"),
            Err(e) => e,
        };
        assert!(
            matches!(err, ArrowError::ArrowError(_)),
            "expected propagated arrow error, got {err:?}"
        );
    }

    #[test]
    fn decimal_extractor_preserves_declared_scale() {
        for dt in [DataType::Decimal128(10, 4), DataType::Decimal256(60, 4)] {
            let e = ColumnExtractor::new(&dt, 8).unwrap();
            let arr = e.finish();
            assert_eq!(arr.data_type(), &dt, "extractor must not alter the type");
        }
    }

    #[test]
    fn oracle_unconstrained_number_maps_to_valid_decimal() {
        use arrow::datatypes::DataType as DT;
        // Oracle reports data_precision = 0 for unconstrained NUMBER.
        let dt = map_oracle_type_to_arrow("NUMBER", Some(0), Some(0));
        assert_eq!(dt, DT::Decimal128(38, 0));
        ColumnExtractor::new(&dt, 4).expect("must be constructible");
    }

    #[test]
    fn oracle_exponent_notation_number_maps_to_float64() {
        use arrow::datatypes::DataType as DT;
        assert_eq!(
            map_oracle_type_to_arrow("NUMBER", Some(5), Some(-3)),
            DT::Float64
        );
        assert_eq!(
            map_oracle_type_to_arrow("NUMBER", None, Some(-2)),
            DT::Float64
        );
    }

    #[test]
    fn oracle_number_with_scale_exceeding_precision_is_clamped() {
        use arrow::datatypes::DataType as DT;
        // scale > precision is invalid for Arrow; must not be passed through.
        let dt = map_oracle_type_to_arrow("NUMBER", Some(4), Some(9));
        assert_eq!(dt, DT::Decimal128(4, 0));
        ColumnExtractor::new(&dt, 4).expect("must be constructible");
    }
}
