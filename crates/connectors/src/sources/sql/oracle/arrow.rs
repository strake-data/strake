//! # Oracle Arrow Conversion
//!
//! Provides utilities to convert Oracle result set rows into Arrow record batches.

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Decimal256Builder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    IntervalMonthDayNanoBuilder, IntervalYearMonthBuilder, LargeBinaryBuilder, LargeStringBuilder,
    StringBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, TimeZone, Utc};
use std::sync::Arc;
use thiserror::Error;

use crate::sources::sql::oracle::conn::map_oracle_type_to_arrow;

#[derive(Debug, Error)]
pub enum ArrowError {
    #[error("Oracle error: {0}")]
    OracleError(#[from] rust_oracle::Error),

    #[error("Failed to map column {0} to arrow type")]
    FailedToMapColumnType(String),

    #[error("Failed to downcast builder at index {0}")]
    FailedToDowncastBuilder(usize),

    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
}

pub type Result<T> = std::result::Result<T, ArrowError>;

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
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

    if let Some(schema) = projected_schema {
        for field in schema.fields() {
            arrow_fields.push(field.clone());
            builders.push(make_builder(field.data_type(), rows.len()));
        }
    } else {
        let column_info = first_row.column_info();
        for info in column_info {
            let name = info.name().to_string();
            // rust_oracle's OracleType implements Display.
            let oracle_type_str = info.oracle_type().to_string();

            // For now we pass None for precision and scale to the mapping function
            // since rust_oracle's ColumnInfo might not cleanly expose them depending on the version.
            let data_type = map_oracle_type_to_arrow(&oracle_type_str, None, None);

            arrow_fields.push(Arc::new(Field::new(name, data_type.clone(), true)));
            builders.push(make_builder(&data_type, rows.len()));
        }
    }

    for row in rows {
        for (i, builder) in builders.iter_mut().enumerate() {
            let data_type = arrow_fields[i].data_type();
            append_value(builder, data_type, &row, i)?;
        }
    }

    let columns: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();
    let schema = Arc::new(Schema::new(arrow_fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn make_builder(dt: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match dt {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 10)),
        DataType::LargeUtf8 => Box::new(LargeStringBuilder::with_capacity(capacity, capacity * 10)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, capacity * 10)),
        DataType::LargeBinary => {
            Box::new(LargeBinaryBuilder::with_capacity(capacity, capacity * 10))
        }
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Decimal128(p, s) => Box::new(
            Decimal128Builder::with_capacity(capacity)
                .with_precision_and_scale(*p, *s)
                .unwrap(),
        ),
        DataType::Decimal256(p, s) => Box::new(
            Decimal256Builder::with_capacity(capacity)
                .with_precision_and_scale(*p, *s)
                .unwrap(),
        ),
        DataType::Timestamp(TimeUnit::Second, tz) => {
            Box::new(TimestampSecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => Box::new(
            TimestampMillisecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone()),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => Box::new(
            TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone()),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => Box::new(
            TimestampNanosecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone()),
        ),
        DataType::Interval(IntervalUnit::YearMonth) => {
            Box::new(IntervalYearMonthBuilder::with_capacity(capacity))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            Box::new(IntervalMonthDayNanoBuilder::with_capacity(capacity))
        }
        _ => Box::new(StringBuilder::with_capacity(capacity, capacity * 10)),
    }
}

fn append_value(
    builder: &mut Box<dyn ArrayBuilder>,
    dt: &DataType,
    row: &rust_oracle::Row,
    i: usize,
) -> Result<()> {
    match dt {
        DataType::Boolean => {
            let b: Option<bool> = row.get(i)?;
            builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?
                .append_option(b);
        }
        DataType::Int32 => {
            let v: Option<i32> = row.get(i)?;
            builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?
                .append_option(v);
        }
        DataType::Int64 => {
            let v: Option<i64> = row.get(i)?;
            builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?
                .append_option(v);
        }
        DataType::Float32 => {
            let v: Option<f32> = row.get(i)?;
            builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?
                .append_option(v);
        }
        DataType::Float64 => {
            let v: Option<f64> = row.get(i)?;
            builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?
                .append_option(v);
        }
        DataType::Utf8 => {
            let v: Option<String> = row.get(i)?;
            builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?
                .append_option(v);
        }
        DataType::LargeUtf8 => {
            let v: Option<String> = row.get(i)?;
            builder
                .as_any_mut()
                .downcast_mut::<LargeStringBuilder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?
                .append_option(v);
        }
        DataType::Date32 => {
            let v: Option<rust_oracle::sql_type::Timestamp> = row.get(i)?;
            let date_builder = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?;
            if let Some(ts) = v {
                if let Some(date) = NaiveDate::from_ymd_opt(ts.year(), ts.month(), ts.day()) {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let days = date.signed_duration_since(epoch).num_days() as i32;
                    date_builder.append_value(days);
                } else {
                    date_builder.append_null();
                }
            } else {
                date_builder.append_null();
            }
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let v: Option<rust_oracle::sql_type::Timestamp> = row.get(i)?;
            let ts_builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampSecondBuilder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?;
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
                    ts_builder.append_value(c.timestamp());
                } else {
                    ts_builder.append_null();
                }
            } else {
                ts_builder.append_null();
            }
        }
        DataType::Decimal128(_, s) => {
            // Fetch as string to maintain exact precision instead of going through f64
            let v: Option<String> = row.get(i)?;
            let dec_builder = builder
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?;
            if let Some(val_str) = v {
                // Parse exact string into decimal i128
                let parsed = parse_decimal128(&val_str, *s);
                if let Some(scaled) = parsed {
                    dec_builder.append_value(scaled);
                } else {
                    dec_builder.append_null();
                }
            } else {
                dec_builder.append_null();
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let v: Option<rust_oracle::sql_type::Timestamp> = row.get(i)?;
            let ts_builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?;
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
                    ts_builder.append_value(c.timestamp_millis());
                } else {
                    ts_builder.append_null();
                }
            } else {
                ts_builder.append_null();
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let v: Option<rust_oracle::sql_type::Timestamp> = row.get(i)?;
            let ts_builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?;
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
                    ts_builder.append_value(c.timestamp_micros());
                } else {
                    ts_builder.append_null();
                }
            } else {
                ts_builder.append_null();
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let v: Option<rust_oracle::sql_type::Timestamp> = row.get(i)?;
            let ts_builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampNanosecondBuilder>()
                .ok_or_else(|| ArrowError::FailedToDowncastBuilder(i))?;
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
                    ts_builder.append_value(c.timestamp_nanos_opt().unwrap_or(0));
                } else {
                    ts_builder.append_null();
                }
            } else {
                ts_builder.append_null();
            }
        }
        _ => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                let v: Option<String> = row.get(i).ok().flatten();
                b.append_option(v);
            } else {
                return Err(ArrowError::FailedToMapColumnType(format!("{:?}", dt)));
            }
        }
    }
    Ok(())
}

fn parse_decimal128(s: &str, scale: i8) -> Option<i128> {
    // Basic exact parser for decimals
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
            return None; // Invalid character
        }
    }

    if integer_part.is_empty() {
        integer_part.push('0');
    }

    // Pad or truncate fractional part to match the scale
    let scale_usize = scale as usize;
    if fractional_part.len() > scale_usize {
        let round_pos = scale_usize;
        let round_digit = fractional_part.as_bytes()[round_pos] - b'0';
        fractional_part.truncate(scale_usize);

        if round_digit >= 5 {
            // Round up the truncated result
            let full_str = format!("{}{}", integer_part, fractional_part);
            if let Ok(mut val) = full_str.parse::<i128>() {
                val += 1;
                return if is_negative { Some(-val) } else { Some(val) };
            }
        }
    } else {
        while fractional_part.len() < scale_usize {
            fractional_part.push('0');
        }
    }

    let full_str = format!("{}{}", integer_part, fractional_part);
    let val = full_str.parse::<i128>().ok()?;

    if is_negative { Some(-val) } else { Some(val) }
}
