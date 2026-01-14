use calamine::Data;
use phf::phf_map;
use polars_core::datatypes::AnyValue;
use polars_core::prelude::DataType;
use std::fmt::Error;

pub static DT_CONV_MAP: phf::Map<&'static str, DataType> = phf_map! {
    "null" | "Null" | "NULL" | "x" | "X" | "remove" | "Remove" => DataType::Null,
    "bool" | "Bool" | "Boolean" | "boolean" | "BOOL" | "BOOLEAN"  => DataType::Boolean,
    "u8" | "U8" | "uint8" | "UInt8" | "UINT8" | "bit" | "Bit" | "BIT"  => DataType::UInt8,
    "u16" | "U16" | "uint16" | "UInt16" | "UINT16"  => DataType::UInt16,
    "u32" | "U32" | "uint32" | "UInt32" | "UINT32" | "Int" | "INT" | "integer" | "Integer" | "INTEGER"  => DataType::UInt32,
    "u64" | "U64" | "uint64" | "UInt64" | "UINT64" => DataType::UInt64,
    "u128" | "U128" | "uint128" | "UInt128" | "UINT128"  => DataType::UInt128,
    "i8" | "I8" | "int8" | "Int8" | "INT8" | "tinyint" | "TinyInt"  => DataType::Int8,
    "i16" | "I16" | "int16" | "Int16" | "INT16"  => DataType::Int16,
    "i32" | "I32" | "int32" | "Int32" | "INT32"  => DataType::Int32,
    "i64" | "I64" | "int64" | "Int64" | "INT64"  => DataType::Int64,
    "i128" | "I128" | "int128" | "Int128" | "INT128"  => DataType::Int128,
    "f32" | "F32" | "float32" | "Float32" | "FLOAT32"  => DataType::Float32,
    "f64" | "F64" | "float64" | "Float64" | "FLOAT64" | "float" | "Float" | "FLOAT" | "decimal" | "Decimal" | "DECIMAL"  => DataType::Float64,
    "str" | "Str" | "string" | "String" | "STRING" | "TEXT"  => DataType::String,
    "date" | "Date" | "DATE"  => DataType::Date,
};

pub fn cast_excel_type_to_polars_type(
    value: &calamine::Data,
    dtype: &DataType,
    column: &mut Vec<AnyValue>,
) -> Result<(), Box<dyn std::error::Error>> {
    match value {
        calamine::Data::Empty => {
            column.push(AnyValue::Null);
        }
        Data::Int(i) => match dtype {
            DataType::UInt8 => {
                column.push(AnyValue::UInt8(*i as u8));
            }
            DataType::UInt16 => {
                column.push(AnyValue::UInt16(*i as u16));
            }
            DataType::UInt32 => {
                column.push(AnyValue::UInt32(*i as u32));
            }
            DataType::UInt64 => {
                column.push(AnyValue::UInt64(*i as u64));
            }
            DataType::UInt128 => {
                column.push(AnyValue::UInt128(*i as u128));
            }
            DataType::Int8 => {
                column.push(AnyValue::Int8(*i as i8));
            }
            DataType::Int16 => {
                column.push(AnyValue::Int16(*i as i16));
            }
            DataType::Int32 => {
                column.push(AnyValue::Int32(*i as i32));
            }
            DataType::Int64 => {
                column.push(AnyValue::Int64(*i as i64));
            }
            DataType::Int128 => {
                column.push(AnyValue::Int128(*i as i128));
            }
            DataType::Boolean => {
                column.push(AnyValue::Boolean(*i != 0));
            }
            DataType::Float32 => {
                column.push(AnyValue::Float32(*i as f32));
            }
            DataType::Float64 => {
                column.push(AnyValue::Float64(*i as f64));
            }
            val => {
                panic!("Mismatched data type for Int value: {val}");
            }
        },
        Data::Float(f) => {
            column.push(AnyValue::Float64(*f));
        }
        Data::String(s) => {
            column.push(AnyValue::StringOwned(s.into()));
        }
        Data::Bool(b) => {
            column.push(AnyValue::Boolean(*b));
        }
        Data::DateTime(dt) => match dt.as_datetime().map(|val| val.date()) {
            Some(date) => {
                column.push(AnyValue::Date(date.to_epoch_days()));
            }
            None => {
                column.push(AnyValue::Null);
            }
        },
        Data::Error(e) => {
            #[cfg(debug_assertions)]
            {
                eprintln!("Error reading cell {e}");
            }
            column.push(AnyValue::Null);
        }
        _unknown_type => {
            return Err(Box::new(Error {}));
        }
    }
    Ok(())
}
