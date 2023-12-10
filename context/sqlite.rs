use crate::page_header::PageHeader;
use crate::sqlite_header::{SQLiteHeader, TextEncoding};
use crate::sqlite_value::Value;
use crate::varint::read_varint;
use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;
use nom::multi::count;
use nom::number::streaming::{be_u16, be_u32};
use nom::IResult;
use std::cmp::Reverse;
use std::fs::File;
use std::io::SeekFrom;
use std::io::{Read, Seek};

pub struct SQLite {
  pub file: File,
  pub header: SQLiteHeader,
  pub pages: Box<[LazyPage]>,
}

impl SQLite {
  pub fn new(mut file: File) -> Result<Self> {
    file.seek(std::io::SeekFrom::Start(0))?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;
    let header = SQLiteHeader::from(&header)?;
    let pages = vec![LazyPage::default(); header.db_page_count as usize].into_boxed_slice();
    Ok(Self {
      file,
      header,
      pages,
    })
  }
  pub fn page(&mut self, index: usize) -> Result<&Page> {
    self.pages[index].load(&mut self.file, &self.header, index)
  }
}

#[derive(Debug, Clone, Default)]
pub enum LazyPage {
  #[default]
  Unloaded,
  Loaded(Page),
}
impl<'a> LazyPage {
  fn load(&mut self, file: &mut File, db_header: &SQLiteHeader, page_number: usize) -> Result<&Page> {
    match self {
      LazyPage::Loaded(page) => Ok(page),
      LazyPage::Unloaded => {
        let page_size = db_header.page_size as usize;
        let file_offset = page_number * page_size;
        file.seek(SeekFrom::Start(u64::try_from(file_offset)?))?;
        let mut page = vec![0; page_size];
        if let Err(_) = file.read_exact(&mut page) {
          bail!("Failed to read a full page")
        }
        let page = &page[..page_size - db_header.reserved_size];
        let page_start = if page_number == 0 { 100usize } else { 0usize };
        let page = Page::from_bytes(db_header.text_encoding, page, page_start)?;
        *self = Self::Loaded(page);
        let LazyPage::Loaded(page) = self else {
          unreachable!()
        };
        Ok(page)
      }
    }
  }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Page {
  pub page_type: PageType,
  pub cells: Box<[Cell]>,
}
impl Page {
  fn from_bytes(text_encoding: TextEncoding, page: &[u8], header_start: usize) -> Result<Self> {
    if page.len() < header_start + PageHeader::size_of() {
      bail!("Page of length {} is not big enough", page.len());
    }
    let content = &page[header_start..];
    let (header, mut content) = content.split_at(PageHeader::size_of());
    let header = PageHeader::from(&header)?;
    let mut right_most_pointer: Option<u32> = None;
    let page_type = match header.page_type {
      0x02 => {
        let (content_, right_most_pointer_) = child_page(content).map_err(|e: nom::Err<nom::error::Error<&[u8]>>| e.to_owned())?;
        content = content_;
        right_most_pointer = Some(right_most_pointer_ - 1);
        PageType::InteriorIndex
      }
      0x05 => {
        let (content_, right_most_pointer_) = child_page(content).map_err(|e: nom::Err<nom::error::Error<&[u8]>>| e.to_owned())?;
        content = content_;
        right_most_pointer = Some(right_most_pointer_ - 1);
        PageType::InteriorTable
      }
      0x0a => PageType::LeafIndex,
      0x0d => PageType::LeafTable,
      otherwise => bail!("Invalid page type: {:02x}", otherwise),
    };
    let cells_count = usize::from(header.cells_count);
    let start_cell_content_area = usize::from(header.start_cell_content_area);
    if content.len() < cells_count * 2 || page.len() < start_cell_content_area {
      bail!("Page of length {} is not big enough", page.len());
    }
    fn get_cell_indices(s: &[u8]) -> Result<Vec<usize>> {
      let repeat = s.len() / 2;
      let (_, result) = count(be_u16, repeat)(s).map_err(|e: nom::Err<nom::error::Error<&[u8]>>| e.to_owned())?;
      Ok(result.iter().map(|i| usize::from(*i)).collect())
    }
    let mut cell_positions = get_cell_indices(&content[..cells_count * 2])?;
    cell_positions.sort_unstable_by_key(|o| Reverse(*o));
    match page_type {
      PageType::LeafTable => {
        let mut cells: Vec<LeafTableCell> = Vec::with_capacity(cells_count);
        let mut page = page;
        for index in cell_positions {
          ensure!(index >= start_cell_content_area, "Cell offset {} cannot be before content start ({})", index,start_cell_content_area);
          let (remaining, cell) = page.split_at(index);
          page = remaining;
          let cell = LeafTableCell::from_bytes(text_encoding, cell)?;
          cells.push(cell);
        }
        let cells: Vec<Cell> = cells.into_iter().map(|cell| Cell::LeafTable(cell)).collect();
        Ok(Self {
            page_type,
            cells: cells.into_boxed_slice(),
        })
      }
      PageType::LeafIndex => {
        let mut cells: Vec<LeafIndexCell> = Vec::with_capacity(cells_count);
        let mut page = page;
        for index in cell_positions {
          ensure!(index >= start_cell_content_area, "Cell offset {} cannot be before content start ({})", index, start_cell_content_area);
          let (remaining, cell) = page.split_at(index);
          page = remaining;
          let cell = LeafIndexCell::from_bytes(text_encoding, cell)?;
          cells.push(cell);
        }
        let cells: Vec<Cell> = cells.into_iter().map(|cell| Cell::LeafIndex(cell)).collect();
        Ok(Self {
          page_type,
          cells: cells.into_boxed_slice(),
        })
      }
      PageType::InteriorTable => {
        let mut cells: Vec<InteriorTableCell> = Vec::with_capacity(cells_count);
        let mut page = page;
        for index in cell_positions {
          ensure!(index >= start_cell_content_area, "Cell offset {} cannot be before content start ({})", index, start_cell_content_area);
          let (remaining, cell) = page.split_at(index);
          page = remaining;
          let cell = InteriorTableCell::from_bytes(cell)?;
          cells.push(cell);
        }
        cells.push(InteriorTableCell {
          left_ptr: u64::max_value(),
          child_page: right_most_pointer.unwrap(),
        });
        let cells: Vec<Cell> = cells.into_iter().sorted_by(|a, b| a.left_ptr.cmp(&b.left_ptr)).map(|cell| Cell::InteriorTable(cell)).collect();
        Ok(Self {
          page_type,
          cells: cells.into_boxed_slice(),
        })
      }
      PageType::InteriorIndex => {
        let mut cells: Vec<InteriorIndexCell> = Vec::with_capacity(cells_count);
        let mut page = page;
        for index in cell_positions {
          ensure!(index >= start_cell_content_area, "Cell offset {} cannot be before content start ({})", index, start_cell_content_area);
          let (remaining, cell) = page.split_at(index);
          page = remaining;
          let cell = InteriorIndexCell::from_bytes(text_encoding, cell)?;
          cells.push(cell);
        }
        cells.push(InteriorIndexCell {
          row_id: u64::max_value(),
          child_page: right_most_pointer.unwrap(),
          payload: Record {
            values: vec![].into_boxed_slice(),
          },
          overflow: None,
        });
        let cells: Vec<Cell> = cells.into_iter().map(|cell| Cell::InteriorIndex(cell)).collect();
        Ok(Self {
          page_type,
          cells: cells.into_boxed_slice(),
        })
      }
    }
  }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
  LeafTable(LeafTableCell),
  LeafIndex(LeafIndexCell),
  InteriorTable(InteriorTableCell),
  InteriorIndex(InteriorIndexCell),
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafTableCell {
    pub row_id: u64,
    pub payload: Record,
    pub overflow: Option<Overflow>,
}
impl LeafTableCell {
  fn from_bytes(text_encoding: TextEncoding, bytes: &[u8]) -> Result<Self> {
    let (payload_size, bytes, _) = read_varint(bytes)?;
    let payload_size = usize::try_from(payload_size).map_err(|_| anyhow!("Payload size is too big: {}", payload_size))?;
    let (row_id, mut bytes, _) = read_varint(bytes)?;
    let mut overflow = None;
    if payload_size > bytes.len() {
      let overflow_point = bytes.len().checked_sub(4).ok_or_else(|| {
          anyhow!("Invalid payload size {} (need at least 4 bytes, but only got {})", payload_size, bytes.len())
      })?;
      let (payload, overflow_page) = bytes.split_at(overflow_point);
      let overflow_page = u32::from_be_bytes(overflow_page.try_into()?);
      overflow = Some(Overflow {
        page: overflow_page,
        spilled_length: payload_size - payload.len(),
      });
      bytes = payload;
    }
    let payload = Record::from_bytes(text_encoding, row_id, bytes)?;
    Ok(Self {
      row_id,
      payload,
      overflow,
    })
  }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InteriorIndexCell {
  pub row_id: u64,
  pub child_page: u32,
  pub payload: Record,
  pub overflow: Option<Overflow>,
}
impl InteriorIndexCell {
  fn from_bytes(text_encoding: TextEncoding, bytes: &[u8]) -> Result<Self> {
    let (bytes, child_page) = child_page(bytes).map_err(|e: nom::Err<nom::error::Error<&[u8]>>| e.to_owned())?;
    let child_page = child_page - 1;
    let (payload_size, mut bytes, _) = read_varint(bytes)?;
    let payload_size = usize::try_from(payload_size).map_err(|_| anyhow!("Payload size is too big: {}", payload_size))?;
    let mut overflow = None;
    if payload_size > bytes.len() {
      let overflow_point = bytes.len().checked_sub(4).ok_or_else(|| {
        anyhow!("Invalid payload size {} (need at least 4 bytes, but only got {})", payload_size, bytes.len())
      })?;
      let (payload, overflow_page) = bytes.split_at(overflow_point);
      let overflow_page = u32::from_be_bytes(overflow_page.try_into()?);
      overflow = Some(Overflow {
        page: overflow_page,
        spilled_length: payload_size - payload.len(),
      });
      bytes = payload;
    }
    let payload = Record::from_bytes(text_encoding, 0, bytes)?;
    let row_id = match payload.values.last() {
        Some(Value::Int(row_id)) => u64::try_from(*row_id)?, _ => return Err(anyhow!("Expected integer row id, but got {:?}", payload)),
    };
    let payload = Record {
      values: payload.values[..payload.values.len() - 1].to_vec().into_boxed_slice(),
    };
    Ok(Self {
      row_id,
      child_page,
      payload,
      overflow,
    })
  }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafIndexCell {
  pub row_id: u64,
  pub payload: Record,
  pub overflow: Option<Overflow>,
}
impl LeafIndexCell {
  fn from_bytes(text_encoding: TextEncoding, bytes: &[u8]) -> Result<Self> {
    let (payload_size, mut bytes, _) = read_varint(bytes)?;
    let payload_size = usize::try_from(payload_size).map_err(|_| anyhow!("Payload size is too big: {}", payload_size))?;
    let mut overflow = None;
    if payload_size > bytes.len() {
      let overflow_point = bytes.len().checked_sub(4).ok_or_else(|| {
        anyhow!("Invalid payload size {} (need at least 4 bytes, but only got {})", payload_size,  bytes.len())
      })?;
      let (payload, overflow_page) = bytes.split_at(overflow_point);
      let overflow_page = u32::from_be_bytes(overflow_page.try_into()?);
      overflow = Some(Overflow {
        page: overflow_page,
        spilled_length: payload_size - payload.len(),
      });
      bytes = payload;
    }
    let payload = Record::from_bytes(text_encoding, 0, bytes)?;
    let row_id = match payload.values.last() {
      Some(Value::Int(row_id)) => u64::try_from(*row_id)?, _ => return Err(anyhow!("Expected integer row id, but got {:?}", payload)),
    };
    let payload = Record {
      values: payload.values[..payload.values.len() - 1].to_vec().into_boxed_slice(),
    };
    Ok(Self {
      row_id,
      payload,
      overflow,
    })
  }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InteriorTableCell {
  pub left_ptr: u64,
  pub child_page: u32,
}
impl InteriorTableCell {
  fn from_bytes(bytes: &[u8]) -> Result<Self> {
    let (bytes, child_page) = child_page(bytes).map_err(|e: nom::Err<nom::error::Error<&[u8]>>| e.to_owned())?;
    let child_page = child_page - 1;
    let (key, _, _) = read_varint(bytes)?;
    Ok(Self {
      left_ptr: key,
      child_page,
    })
  }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Overflow {
  pub page: u32,
  pub spilled_length: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub values: Box<[Value]>,
}
impl Record {
  fn from_bytes(text_encoding: TextEncoding, row_id: u64, bytes: &[u8]) -> Result<Self> {
    let (header_size, bytes, header_size_varint_bytes) = read_varint(bytes)?;
    let (mut header, mut bytes) = bytes.split_at(usize::try_from(header_size)? - header_size_varint_bytes);
    let mut values = Vec::new();
    while !header.is_empty() {
      let (serial_type, rest, _) = read_varint(header)?;
      header = rest;
      let (mut value, rest) = Value::from_bytes(text_encoding, serial_type, bytes)?;
      bytes = rest;
      if let Value::Null = value {
        if values.is_empty() {
          value = Value::Int(row_id as i64);
        }
      }
      values.push(value);
    }
    Ok(Self {
      values: values.into_boxed_slice(),
    })
  }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PageType {
  InteriorIndex = 0x02,
  InteriorTable = 0x05,
  LeafIndex = 0x0a,
  LeafTable = 0x0d,
}
fn child_page(input: &[u8]) -> IResult<&[u8], u32> {
  be_u32(input)
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn test_interior_table_cell_from_bytes() {
    let bytes = [0x00, 0x00, 0x00, 0x02, 0x81, 0x04];
    let expected = InteriorTableCell {
      left_ptr: 132,
      child_page: 1,
    };
    let result = InteriorTableCell::from_bytes(&bytes);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), expected);
  }

  #[test]
  fn test_interior_table_cell_from_bytes_invalid_length() {
    let bytes = [0x00, 0x00, 0x00];
    let result = InteriorTableCell::from_bytes(&bytes);
    assert!(result.is_err());
  }

  #[test]
  fn test_interior_table_cell_from_bytes_with_extra_data() {
    let bytes = [0x00, 0x00, 0x00, 0x03, 0x81, 0x04, 0xFF];
    let expected = InteriorTableCell {
        left_ptr: 132,
        child_page: 2,
    };
    let result = InteriorTableCell::from_bytes(&bytes);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), expected);
  }
}