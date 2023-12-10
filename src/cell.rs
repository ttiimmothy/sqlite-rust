use nom::{number::complete::be_u32, IResult};

use crate::{
    page::BTreePageType,
    record::{Record, RecordType},
    varint::varint,
};

#[allow(dead_code)]
#[derive(Debug)]
pub enum Cell {
    TableLeaf(Record),
    TableInterior {
        left_child_pointer: u32,
        key: i64,
    },
    IndexLeaf(Record),
    IndexInterior {
        left_child_pointer: u32,
        record: Record,
    },
}

impl Cell {
    pub fn parse<'input>(
        input: &'input [u8],
        ty: BTreePageType,
        usable_page_size: usize,
        column_names: &[&str],
        column_indices: &[usize],
    ) -> IResult<&'input [u8], Self> {
        let (input, left_child_pointer) = if matches!(ty, BTreePageType::IndexInterior) {
            let (input, left_child_pointer) = be_u32(input)?;
            (input, Some(left_child_pointer))
        } else {
            (input, None)
        };

        // Check for overflow
        let input = if matches!(
            ty,
            BTreePageType::TableLeaf | BTreePageType::IndexInterior | BTreePageType::IndexLeaf
        ) {
            let (input, payload_size) = varint(input)?;
            let payload_size = payload_size as usize;

            let x = match ty {
                BTreePageType::TableLeaf => usable_page_size - 35,
                _ => ((usable_page_size - 12) * 64 / 255) - 23,
            };
            let m = ((usable_page_size - 12) * 32 / 255) - 23;
            let k = m + ((payload_size - m) % (usable_page_size - 4));

            if payload_size > x {
                // Overflow
                let overflow_size = if k <= x {
                    payload_size - k
                } else {
                    payload_size - m
                };
                todo!("overflow of size {:?}", overflow_size);
            }

            input
        } else {
            input
        };

        match ty {
            BTreePageType::TableInterior => {
                let (input, left_child_pointer) = be_u32(input)?;
                let (input, key) = varint(input)?;
                Ok((
                    input,
                    Cell::TableInterior {
                        left_child_pointer,
                        key,
                    },
                ))
            }
            BTreePageType::TableLeaf => {
                let (input, record) =
                    Record::parse(input, column_names, column_indices, RecordType::Table)?;
                Ok((input, Cell::TableLeaf(record)))
            }
            BTreePageType::IndexInterior => {
                let left_child_pointer = left_child_pointer.unwrap();
                let (input, record) =
                    Record::parse(input, column_names, column_indices, RecordType::Index)?;
                Ok((
                    input,
                    Cell::IndexInterior {
                        left_child_pointer,
                        record,
                    },
                ))
            }
            BTreePageType::IndexLeaf => {
                let (input, record) =
                    Record::parse(input, column_names, column_indices, RecordType::Index)?;
                Ok((input, Cell::IndexLeaf(record)))
            }
        }
    }

    pub fn as_record(&self) -> Option<&Record> {
        match self {
            Cell::TableLeaf(record) => Some(record),
            _ => None,
        }
    }
}
