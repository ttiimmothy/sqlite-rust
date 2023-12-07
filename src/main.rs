use anyhow::{bail, Result};
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
      let page_size = u16::from_be_bytes([header[16], header[17]]);
      let mut first_page = vec![0u8; page_size.into()].into_boxed_slice();
      let mut handle = file.take(page_size.into());
      handle.read(&mut first_page).unwrap();
      let number_of_tables = u16::from_be_bytes([first_page[3], first_page[4]]);
      println!("database page size: {}", page_size);
      println!("number of tables: {}", number_of_tables);
    }
    _ => bail!("Missing or invalid command passed: {}", command),
  }
  Ok(())
}