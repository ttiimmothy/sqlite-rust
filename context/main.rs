use anyhow::{bail, Result};
use sqlite_starter_rust::db;
use sqlite_starter_rust::page_header::PageHeader;
use sqlite_starter_rust::sqlite::{Cell, SQLite};
use sqlite_starter_rust::sqlite_header::SQLiteHeader;
use sqlite_starter_rust::sqlite_table::{CreateTable, TableType};
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
  let args = std::env::args().collect::<Vec<_>>();
  match args.len() {
    0 | 1 => bail!("Missing <database path> and <command>"),
    2 => bail!("Missing <command>"),
    _ => {}
  }
  
  let command = &args[2];
  match command.as_str() {
    ".dbinfo" => {
      let mut file = File::open(&args[1])?;
      let mut header = [0; 100];
      file.read_exact(&mut header)?;
      let file_header = SQLiteHeader::from(&header)?;
      file_header.print();
      let mut page_header = [0; 8];
      file.read_exact(&mut page_header)?;
      let page_header = PageHeader::from(&page_header)?;
      page_header.print();
    }
    ".tables" => {
      let file = File::open(&args[1])?;
      let mut db = SQLite::new(file)?;
      let root_page = db.page(0)?;
      let table_names = root_page
        .cells
        .iter()
        .map(|cell| match cell {
          Cell::LeafTable(cell) => CreateTable::from_record(&cell.payload),
          Cell::InteriorTable(_) => todo!("Interior tables not yet supported"),
          Cell::LeafIndex(_) => todo!("Leaf Index tables not yet supported"),
          Cell::InteriorIndex(_) => todo!("Interior Index tables not yet supported"),
        })
        .filter_map(|result| match result {
          Ok(create_table) => Some(create_table),
          Err(_) => None,
        })
        .filter(|create_table| matches!(create_table.table_type, TableType::Table))
        .map(|create_table| create_table.table_name)
        .collect::<Vec<_>>();
      println!("{}", table_names.join(" "));
    }
    query => {
      let result = db::process_sql(&args[1], query)?;
      println!("{result}");
    }
  }
  Ok(())
}