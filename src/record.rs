use std::fmt::Display;
use nom::{bytes::complete::take, number::complete::i8, IResult};

use crate::varint::varint;

#[derive(Debug)]
pub struct Record {
  pub values: Vec<Value>,
}

#[derive(Debug)]
pub enum ColumnType {
  Null,
  I8,
  I16,
  I24,
  I32,
  I48,
  I64,
  F64,
  Zero,
  One,
  Blob(usize),
  Text(usize),
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum Value {
  Null,
  Integer(i64),
  Real(f64),
  Text(String),
  Blob(String),
}

impl PartialOrd for Value {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    match self {
      Value::Null => None,
      Value::Integer(n1) => match other {
        Value::Integer(n2) => n1.partial_cmp(n2),
        _ => None,
      },
      Value::Real(f1) => match other {
        Value::Real(f2) => f1.partial_cmp(f2),
        _ => None,
      },
      Value::Text(s1) => match other {
        Value::Text(s2) => s1.partial_cmp(s2),
        Value::Blob(s2) => s1.partial_cmp(s2),
        _ => None,
      },
      Value::Blob(s1) => match other {
        Value::Text(s2) => s1.partial_cmp(s2),
        Value::Blob(s2) => s1.partial_cmp(s2),
        _ => None,
      },
    }
  }
}

impl PartialEq for Value {
  fn eq(&self, other: &Self) -> bool {
    match self {
      Value::Null => false,
      Value::Integer(n1) => match other {
        Value::Integer(n2) => n1 == n2,
        _ => false,
      },
      Value::Real(f1) => match other {
        Value::Real(f2) => f1 == f2,
        _ => false,
      },
      Value::Text(s1) => match other {
        Value::Text(s2) => s1 == s2,
        Value::Blob(s2) => s1 == s2,
        _ => false,
      },
      Value::Blob(s1) => match other {
        Value::Text(s2) => s1 == s2,
        Value::Blob(s2) => s1 == s2,
        _ => false,
      },
    }
  }
}

impl Value {
  pub fn as_integer(&self) -> Option<i64> {
    match self {
      Value::Integer(n) => Some(*n),
      _ => None,
    }
  }

  #[allow(dead_code)]
  pub fn as_real(&self) -> Option<f64> {
    match self {
      Value::Real(f) => Some(*f),
      _ => None,
    }
  }

  pub fn as_text(&self) -> Option<&str> {
    match self {
      Value::Text(s) => Some(s),
      _ => None,
    }
  }

  #[allow(dead_code)]
  pub fn as_blob(&self) -> Option<&str> {
    match self {
      Value::Blob(s) => Some(s),
      _ => None,
    }
  }
}

impl Display for Value {
  fn fmt(&self, f1: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let str = match self {
      Value::Null => "null".into(),
      Value::Integer(n) => n.to_string(),
      Value::Real(f) => f.to_string(),
      Value::Blob(s) => s.to_owned(),
      Value::Text(s) => s.to_owned(),
    };
    write!(f1, "{}", str)
  }
}

impl ColumnType {
  #[allow(dead_code)]
  fn size(&self) -> usize {
    match self {
      ColumnType::Null => 0,
      ColumnType::I8 => 1,
      ColumnType::I16 => 2,
      ColumnType::I24 => 3,
      ColumnType::I32 => 4,
      ColumnType::I48 => 6,
      ColumnType::I64 => 8,
      ColumnType::F64 => 8,
      ColumnType::Zero => 0,
      ColumnType::One => 0,
      ColumnType::Blob(size) | ColumnType::Text(size) => *size,
    }
  }
}

impl TryFrom<i64> for ColumnType {
  type Error = anyhow::Error;

  fn try_from(value: i64) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(ColumnType::Null),
      1 => Ok(ColumnType::I8),
      2 => Ok(ColumnType::I16),
      3 => Ok(ColumnType::I24),
      4 => Ok(ColumnType::I32),
      5 => Ok(ColumnType::I48),
      6 => Ok(ColumnType::I64),
      7 => Ok(ColumnType::F64),
      8 => Ok(ColumnType::Zero),
      9 => Ok(ColumnType::One),
      10 | 11 => Err(anyhow::format_err!("invalid column type")),
      value => {
        if value % 2 == 0 {
          Ok(ColumnType::Blob(((value - 12) / 2) as usize))
        } else {
          Ok(ColumnType::Text(((value - 13) / 2) as usize))
        }
      }
    }
  }
}

#[derive(Debug, PartialEq)]
pub enum RecordType {
  Table,
  Index,
}

impl Record {
  pub fn parse<'input>(
    input: &'input [u8],
    column_names: &[&str],
    column_indices: &[usize],
    record_type: RecordType,
  ) -> IResult<&'input [u8], Self> {
    let (input, row_id) = if record_type == RecordType::Table {
      let (input, row_id) = varint(input)?;
      (input, Some(row_id))
    } else {
      (input, None)
    };

    let mut header_bytes_read = 0;
    let before_input_len = input.len();
    let (input, header_size) = varint(input)?;
    let header_size = header_size as usize;
    header_bytes_read += before_input_len - input.len();

    let mut rest = input;
    let mut column_types = Vec::new();
    while header_bytes_read < header_size {
      let (remainder, column_type) = varint(rest)?;
      header_bytes_read += rest.len() - remainder.len();
      rest = remainder;
      let column_type = ColumnType::try_from(column_type).expect("invalid column type");
      column_types.push(column_type);
    }

    let mut values = Vec::with_capacity(column_names.len());
    for (i, column_type) in column_types.iter().enumerate() {
      let to_include = column_indices.contains(&i);
      let is_row_id_alias = if to_include {
        column_names[column_indices.iter().position(|j| i == *j).unwrap()] == "id"
      } else {
        false
      };

      match column_type {
        ColumnType::Null => {
          if to_include {
            if record_type == RecordType::Table && is_row_id_alias {
              values.push(Value::Integer(row_id.unwrap()));
            } else {
              values.push(Value::Null);
            }
          }
        }
        ColumnType::I8 => {
          let (remainder, value) = i8(rest)?;
          rest = remainder;
          if to_include {
            values.push(Value::Integer(value as i64));
          }
        }
        ColumnType::I16 => {
          let (remainder, bytes) = take(2usize)(rest)?;
          rest = remainder;
          if to_include {
            values.push(Value::Integer(
              i16::from_be_bytes([bytes[0], bytes[1]]) as i64
            ));
          }
        }
        ColumnType::I24 => {
          let (remainder, bytes) = take(3usize)(rest)?;
          rest = remainder;
          if to_include {
            values.push(Value::Integer(i32::from_be_bytes([
              0, bytes[0], bytes[1], bytes[2],
            ]) as i64));
          }
        }
        ColumnType::I32 => {
          let (remainder, bytes) = take(4usize)(rest)?;
          rest = remainder;
          if to_include {
            values.push(Value::Integer(i32::from_be_bytes([
              bytes[0], bytes[1], bytes[2], bytes[3],
            ]) as i64));
          }
        }
        ColumnType::I48 => todo!("i48 column"),
        ColumnType::I64 => todo!("i64 column"),
        ColumnType::F64 => todo!("f64 column"),
        ColumnType::Zero => {
          if to_include {
            values.push(Value::Integer(0i64));
          }
        }
        ColumnType::One => {
          if to_include {
            values.push(Value::Integer(0i64));
          }
        }
        ColumnType::Blob(size) => {
          let (remainder, bytes) = take(*size)(rest)?;
          rest = remainder;
          if to_include {
            values.push(Value::Blob(
              std::str::from_utf8(bytes).expect("non utf-8 text").to_owned(),
            ));
          }
        }
        ColumnType::Text(size) => {
          let (remainder, bytes) = take(*size)(rest)?;
          rest = remainder;
          if to_include {
            values.push(Value::Text(
              std::str::from_utf8(bytes).expect("non utf-8 text").to_owned(),
            ));
          }
        }
      }
    }

    Ok((rest, Record { values }))
  }
}
