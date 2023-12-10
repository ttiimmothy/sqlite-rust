use nom::{
    bytes::complete::take,
    number::complete::{be_u16, be_u32, u8},
    Err::Error,
    IResult,
};

pub const HEADER_SIZE: usize = 100;

use crate::error::{InvalidValueError, MyError};

#[derive(Debug)]
pub struct Header {
    /// The database page size in bytes.
    pub page_size: usize,
    /// File format write version.
    pub write_version: FormatVersion,
    /// File format read version.
    pub read_version: FormatVersion,
    /// Bytes of unused "reserved" space at the end of each page. Usually 0.
    pub end_page_reserved_bytes: usize,
    /// File change counter.
    pub file_change_counter: usize,
    /// Size of the database file in pages. The "in-header database size".
    pub size_in_pages: usize,
    /// Page number of the first freelist trunk page.
    pub first_feerlist_trunk_page: usize,
    /// Total number of freelist pages.
    pub num_freelist_pages: usize,
    /// The schema cookie.
    pub schema_cookie: u32,
    /// The schema format number. Supported schema formats are 1, 2, 3, and 4.
    pub schema_format: u32,
    /// Default page cache size.
    pub default_page_cache_size: usize,
    /// The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes,
    /// or zero otherwise.
    pub largest_root_btree_page: usize,
    /// The database text encoding.
    pub text_encoding: TextEncoding,
    /// The "user version" as read and set by the user_version pragma.
    pub user_version: u32,
    /// True for incremental-vacuum mode. False otherwise.
    pub incremental_vacuum_mode: bool,
    /// The "Application ID" set by PRAGMA application_id.
    pub application_id: u32,
    /// The version-valid-for number.
    pub version_valid_for: u32,
    /// SQLITE_VERSION_NUMBER
    pub sqlite_version_number: u32,
}

#[derive(Debug)]
pub enum FormatVersion {
    Legacy = 1,
    WriteAheadLog = 2,
}

impl TryFrom<u8> for FormatVersion {
    type Error = InvalidValueError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
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
        let (input, first_feerlist_trunk_page) = be_u32(input)?;
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
        let text_encoding =
            TextEncoding::try_from(text_encoding).map_err(|e| Error(MyError::from(e)))?;

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
                first_feerlist_trunk_page: first_feerlist_trunk_page as usize,
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
