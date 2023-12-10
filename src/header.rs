use nom::{
  bytes::complete::take,
  number::complete::{be_u16, be_u32, u8},
  Err::Error,
  IResult,
};
use crate::error::{InvalidValueError, MyError};

pub const HEADER_SIZE: usize = 100;

#[derive(Debug)]
pub struct Header {
  pub page_size: usize,
  pub write_version: FormatVersion,
  pub read_version: FormatVersion,
  pub end_page_reserved_bytes: usize,
  pub file_change_counter: usize,
  pub size_in_pages: usize,
  pub first_freelist_trunk_page: usize,
  pub num_freelist_pages: usize,
  pub schema_cookie: u32,
  pub schema_format: u32,
  pub default_page_cache_size: usize,
  pub largest_root_btree_page: usize,
  pub text_encoding: TextEncoding,
  pub user_version: u32,
  pub incremental_vacuum_mode: bool,
  pub application_id: u32,
  pub version_valid_for: u32,
  pub sqlite_version_number: u32,
}

#[derive(Debug)]
pub enum FormatVersion {
  Legacy = 1,
  WriteAheadLog = 2,
}

impl TryFrom<u16> for FormatVersion {
  type Error = InvalidValueError;

  fn try_from(value: u16) -> Result<Self, Self::Error> {
    match value {
      1 => Ok(FormatVersion::Legacy),
      2 => Ok(FormatVersion::WriteAheadLog),
      _ => Err(InvalidValueError(format!(
        "invalid format version value {}",
        value
      ))),
    }
  }
}

#[derive(Debug)]
pub enum TextEncoding {
  Utf8 = 1,
  Utf16le = 2,
  Utf16be = 3,
}

impl TryFrom<u32> for TextEncoding {
  type Error = InvalidValueError;

  fn try_from(value: u32) -> Result<Self, Self::Error> {
    match value {
      1 => Ok(TextEncoding::Utf8),
      2 => Ok(TextEncoding::Utf16le),
      3 => Ok(TextEncoding::Utf16be),
      _ => Err(InvalidValueError(format!(
        "invalid text encoding value {}",
        value
      ))),
    }
  }
}

impl Header {
  pub fn parse(input: &[u8]) -> IResult<&[u8], Self, MyError<&[u8]>> {
    let (input, header_string) = take(16usize)(input)?;
    assert_eq!(header_string, b"SQLite format 3\0");

    let (input, page_size) = {
      let (input, value) = be_u16(input)?;
      (
        input,
        match value {
          1 => 65536,
          _ => {
            if value >= 512 && (value & (value - 1)) == 0 {
              value as u32
            } else {
              return Err(Error(MyError::InvalidValueError(InvalidValueError(
                format!("invalid page size {}", value),
              ))));
            }
          }
        },
      )
    };

    let (input, write_version) = u8(input)?;
    let write_version =
      FormatVersion::try_from(write_version).map_err(|e| Error(MyError::from(e)))?;
    let (input, read_version) = u8(input)?;
    let read_version =
      FormatVersion::try_from(read_version).map_err(|e| Error(MyError::from(e)))?;

    let (input, end_page_reserved_bytes) = u8(input)?;

    let (input, max_embedded_payload_fraction) = u8(input)?;
    assert_eq!(max_embedded_payload_fraction, 64);
    let (input, min_embedded_payload_fraction) = u8(input)?;
    assert_eq!(min_embedded_payload_fraction, 32);
    let (input, leaf_payload_fraction) = u8(input)?;
    assert_eq!(leaf_payload_fraction, 32);

    let (input, file_change_counter) = be_u32(input)?;
    let (input, size_in_pages) = be_u32(input)?;
    let (input, first_freelist_trunk_page) = be_u32(input)?;
    let (input, num_freelist_pages) = be_u32(input)?;
    let (input, schema_cookie) = be_u32(input)?;

    let (input, schema_format) = be_u32(input)?;
    if !(1..=4).contains(&schema_format) {
      return Err(Error(MyError::InvalidValueError(InvalidValueError(
        format!("invalid schema format {}", schema_format),
      ))));
    }

    let (input, default_page_cache_size) = be_u32(input)?;
    let (input, largest_root_btree_page) = be_u32(input)?;
    let (input, text_encoding) = be_u32(input)?;
    let text_encoding = TextEncoding::try_from(text_encoding).map_err(|e| Error(MyError::from(e)))?;
    let (input, user_version) = be_u32(input)?;
    let (input, incremental_vacuum_mode) = be_u32(input)?;
    assert!(incremental_vacuum_mode == 0 || incremental_vacuum_mode == 1);
    let incremental_vacuum_mode = incremental_vacuum_mode != 0;
    let (input, application_id) = be_u32(input)?;
    let (input, zeros) = take(20usize)(input)?;
    assert!(zeros.iter().all(|b| *b == 0));
    let (input, version_valid_for) = be_u32(input)?;
    let (input, sqlite_version_number) = be_u32(input)?;

    Ok((
      input,
      Header {
        page_size: page_size as usize,
        write_version,
        read_version,
        end_page_reserved_bytes: end_page_reserved_bytes as usize,
        file_change_counter: file_change_counter as usize,
        size_in_pages: size_in_pages as usize,
        first_freelist_trunk_page: first_freelist_trunk_page as usize,
        num_freelist_pages: num_freelist_pages as usize,
        schema_cookie,
        schema_format,
        default_page_cache_size: default_page_cache_size as usize,
        largest_root_btree_page: largest_root_btree_page as usize,
        text_encoding,
        user_version,
        incremental_vacuum_mode,
        application_id,
        version_valid_for,
        sqlite_version_number,
      },
    ))
  }
}
