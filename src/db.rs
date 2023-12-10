use crate::sql::{
  self, parse_create_index_sql, parse_create_table_sql, ColumnDefinition, CreateTableStatement, Operator, SelectClause, SelectExpression, WhereClause, WhereValue,
};
use crate::sqlite::{Cell, PageType, SQLite};
use crate::sqlite_table::CreateTable;
use crate::sqlite_table::{CreateTable, TableType};
use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use std::collections::{HashMap, VecDeque};
use std::fs::File;

pub fn process_sql(file: &str, query: &str) -> Result<String> {
  let select_expression = sql::parse_select_sql(query)?;
  let filename = file;
  let file = File::open(file)?;
  let mut db = SQLite::new(file)?;
  let (table_definition, _, column_name_to_definition) = get_table_info(filename, select_expression.clone())?;

  let mut stack = VecDeque::new();
  stack.push_back((table_definition.root_page, 0, u64::MAX));
  let mut filtered_cells: Vec<Cell> = Vec::new();
  let row_ids = process_indices(filename, select_expression.clone())?;
  let where_clause = select_expression.where_clause;

  while let Some(page) = stack.pop_front() {
  while let Some((page, min_row_id, max_row_id)) = stack.pop_front() {
    let page = db.page(page)?;
    match page.page_type {
      PageType::InteriorTable => {
        let mut row_ids = row_ids.clone().map(|ids| {
          ids.into_iter().filter(|id| *id <= max_row_id && *id >= min_row_id).collect_vec()
        });
        let pages = page.cells.iter().filter_map(|cell| match cell {
          Cell::InteriorTable(cell) => match &mut row_ids {
            Some(row_ids) => {
              if row_ids.len() == 0 {
                return None;
              }
              if row_ids.iter().any(|row_id| *row_id <= cell.left_ptr) {
                let min_row_id = *row_ids.first().unwrap();
                row_ids.retain(|id| *id > cell.left_ptr);
                Some((
                  usize::try_from(cell.child_page).unwrap(),
                  min_row_id,
                  cell.left_ptr,
                ))
              } else {
                None
              }
            }
            None => Some((
              usize::try_from(cell.child_page).unwrap(),
              min_row_id,
              max_row_id,
            )),
          },
          Cell::LeafTable(_) => {
            panic!("Unexpected LeafTable in interior page")
          }
          Cell::LeafIndex(_) => {
            panic!("Unexpected LeafIndex for interior page")
          }
          Cell::InteriorIndex(_) => {
            panic!("Unexpected InteriorIndex for interior page")
          }
        })
        .collect_vec();
        stack.extend(pages);
      }
      PageType::InteriorIndex => todo!(),
      PageType::LeafIndex => todo!(),
      PageType::LeafTable => {
        if where_clause.is_none() {
          for cell in page.cells.iter() {
            if let Cell::LeafTable(cell) = cell {
              filtered_cells.push(Cell::LeafTable(cell.clone()));
            }
          }
        } else if let Some(WhereClause {
          ref column,
          operator: Operator::Equal,
          ref value,
        }) = where_clause
        {
          let filter_column_index = column_name_to_definition.get(column).ok_or(anyhow!("Column not found: {}", column))?;
          let tmp = page
            .cells
            .clone()
            .into_iter()
            .filter(|cell| match cell {
              Cell::LeafTable(cell) => match &value {
                WhereValue::Number(n) => {
                  *n.to_string() == cell.payload.values[filter_column_index.index].as_text()
                }
                WhereValue::String(s) => {
                  *s == cell.payload.values[filter_column_index.index].as_text()
                }
              },
              _ => {
                todo!("Table type not supported here")
              }
            })
            .map(|cell| cell.clone())
            .collect_vec();
          for cell in tmp {
            if let Cell::LeafTable(cell) = cell {
              filtered_cells.push(Cell::LeafTable(cell.clone()));
            }
          }
        } else {
            bail!("Unsupported filter in query: {}", query)
        };
      }
    }
  }
  
  match select_expression.select_clause {
    SelectClause::FunctionCall { name, args: _ } if name == "COUNT" => {
      let count = filtered_cells.len();
      Ok(format!("{}", count))
    }
    SelectClause::Columns(columns) => {
      let column_indices: Vec<usize> = columns
        .into_iter()
        .filter_map(|column| column_name_to_definition.get(&column))
        .map(|column| column.index)
        .collect();
      let rows = filtered_cells
        .iter()
        .map(|cell| {
          let mut row = Vec::new();
          for column_index in &column_indices {
            let value = match cell {
              Cell::LeafTable(cell) => cell.payload.values[*column_index].clone(),
              Cell::InteriorTable(_) => {
                panic!("Unexpected InteriorTableCell in last stage of processing")
              }
              Cell::LeafIndex(_) => {
                panic!("Unexpected LeafIndexCell in last stage of processing")
              }
              Cell::InteriorIndex(_) => {
                panic!("Unexpected InteriorIndexCell in last stage of processing")
              }
            };
            row.push(value.as_text());
          }
          row
        })
        .collect::<Vec<_>>();
      Ok(rows.into_iter().map(|row| row.join("|")).join("\n"))
    }
    _ => {
      bail!("Unsupported expression: {}", query)
    }
  }
}

pub fn process_indices(file: &str, select_expression: SelectExpression) -> Result<Option<Vec<u64>>> {
  let file = File::open(file)?;
  let mut db = SQLite::new(file)?;
  let root_page = db.page(0)?;
  let where_clause = select_expression.clone().where_clause;
  let indices: Vec<CreateTable> = root_page
    .cells
    .iter()
    .filter_map(|cell| match cell {
      Cell::LeafTable(cell) => {
        let create_table = CreateTable::from_record(&cell.payload).ok()?;
        if create_table.table_type == TableType::Index {
          Some(create_table)
        } else {
          None
        }
      }
      _ => {
        todo!("Only Leaves are currently supported when parsing indices")
      }
    })
    .collect();
  let index_data = match where_clause {
    Some(ref clause) => {
      let relevant_indices = indices
        .iter()
        .filter(|index| *index.table_name == select_expression.table)
        .filter(|index| {
          let parsed_index = parse_create_index_sql(&(*index.sql)).unwrap();
          parsed_index
            .columns
            .iter()
            .any(|column| *column == clause.column)
        })
        .collect_vec();
      if relevant_indices.len() > 1 {
        bail!("Not supported yet! Multiple indices found for column: {}", clause.column);
      }
      if relevant_indices.is_empty() {
        None
      } else {
        let index = relevant_indices.first().unwrap();
        Some((index.root_page, *relevant_indices.first().unwrap()))
      }
    }
    None => None,
  };

  let (root_index_page, _index_definition) = match index_data {
    Some(data) => data,
    None => return Ok(None),
  };

  let where_clause = where_clause.unwrap();
  let mut stack = VecDeque::new();
  stack.push_back(root_index_page);
  let mut row_ids = Vec::new();

  while let Some(page) = stack.pop_front() {
    let page = db.page(page)?;
    match page.page_type {
      PageType::InteriorIndex => {
        let next_pages = page
          .cells
          .iter()
          .filter_map(|cell| match cell {
            Cell::InteriorIndex(cell) => {
              if let WhereClause {
                column: _,
                operator: Operator::Equal,
                ref value,
              } = where_clause
              {
                let is_last = cell.payload.values.len() == 0;
                let is_next = is_last || match &value {
                  WhereValue::Number(n) => {
                    *n <= cell.payload.values.first().unwrap().as_text()
                  }
                  WhereValue::String(s) => {
                    *s <= cell.payload.values.first().unwrap().as_text()
                  }
                };
                if is_next {
                  Some(usize::try_from(cell.child_page).unwrap())
                } else {
                  None
                }
              } else {
                panic!("Unsupported where_clause filter: {:?}", where_clause);
              }
            }
            Cell::LeafTable(_) => {
              panic!("Unexpected LeafTable in interior index")
            }
            Cell::LeafIndex(_) => {
              panic!("Unexpected LeafIndex for interior index")
            }
            Cell::InteriorTable(_) => {
              panic!("Unexpected InteriorTable for interior index")
            }
          })
          .collect_vec();
        stack.extend(next_pages);
      }
      PageType::InteriorTable => panic!("Unexpected InteriorTable when processing indices"),
      PageType::LeafTable => panic!("Unexpected LeafTable when processing indices"),
      PageType::LeafIndex => {
        if let WhereClause {
          column: _,
          operator: Operator::Equal,
          ref value,
        } = where_clause
        {
          let results = page
            .cells
            .into_iter()
            .filter(|cell| match cell {
                Cell::LeafIndex(cell) => match &value {
                    WhereValue::Number(n) => {
                        *n.to_string() == cell.payload.values.first().unwrap().as_text()
                    }
                    WhereValue::String(s) => {
                        *s == cell.payload.values.first().unwrap().as_text()
                    }
                },
                _ => {
                    panic!("Table type not supported here")
                }
            })
            .collect_vec();
          for result in results {
            match result {
              Cell::LeafIndex(cell) => {
                row_ids.push(cell.row_id);
              }
              _ => {
                panic!("Table type not supported here")
              }
            }
          }
        } else {
            bail!("Unsupported filter");
        };
      }
    }
  }
  row_ids.sort();
  Ok(Some(row_ids))
}

#[derive(Debug, Clone)]
pub struct IndexColumnDefinition {
  pub index: usize,
  pub definition: ColumnDefinition,
}

fn get_table_info(file: &str, select_expression: SelectExpression) -> Result<(CreateTable, CreateTableStatement, HashMap<String, IndexColumnDefinition>)> {
  let file = File::open(file)?;
  let mut db = SQLite::new(file)?;
  let root_page = db.page(0)?;
  let table_definition = root_page
    .cells
    .iter()
    .find_map(|cell| match cell {
      Cell::LeafTable(cell) => {
        let create_table = CreateTable::from_record(&cell.payload).ok()?;
        if *create_table.table_name == select_expression.table {
          Some(create_table)
        } else {
          None
        }
      }
      _ => {
        todo!("Table type not supported for the table definition")
      }
    })
    .ok_or_else(|| anyhow!("Table not found: {}", select_expression.table))?;
  let parsed_table_definition = parse_create_table_sql(&table_definition.sql.clone())?;
  let column_name_to_definition: HashMap<String, IndexColumnDefinition> = parsed_table_definition
    .columns
    .iter()
    .enumerate()
    .map(|(index, column)| {
      (
        column.name.clone(),
        IndexColumnDefinition {
          index,
          definition: column.clone(),
        },
      )
    })
    .collect();
  return Ok((
    table_definition,
    parsed_table_definition,
    column_name_to_definition,
  ));
}

#[cfg(test)]
mod tests {
  use super::*;
  const TEST_DB_LEAF: &'static str = "tests/fixtures/leaf.db";
  const TEST_DB_INTERIOR: &'static str = "tests/fixtures/interior.db";

  #[test]
  fn test_only_leaf_pages_process_sql_select_columns() -> Result<()> {
    let query = "SELECT id, username, age FROM users;";
    let result = process_sql(TEST_DB_LEAF, &query)?;
    let expected = vec!["1|Alice|29", "2|Bob|45", "3|Charlie|15", "4|Dave|105"].join("\n");
    assert_eq!(result, expected);
    Ok(())
  }

  #[test]
  fn test_only_leaf_pages_process_sql_count_function() -> Result<()> {
    let query = "SELECT COUNT(*) FROM users;";
    let result = process_sql(TEST_DB_LEAF, query)?;
    assert_eq!(result, "4");
    Ok(())
  }

  #[test]
  fn test_only_leaf_pages_process_sql_table_not_found() -> Result<()> {
    let query = "SELECT name FROM non_existent_table;";
    let result = process_sql(TEST_DB_LEAF, query);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Table not found: non_existent_table"
    );
    Ok(())
  }

  #[test]
  fn test_only_leaf_pages_process_sql_where_clause_equal_integer() -> Result<()> {
    let query = "SELECT username FROM users WHERE age = 105;";
    let result = process_sql(TEST_DB_LEAF, query)?;
    let expected = vec!["Dave"].join("\n");
    assert_eq!(result, expected);
    Ok(())
  }

  #[test]
  fn test_only_leaf_pages_process_sql_count_with_where_clause_equal_integer() -> Result<()> {
    let query = "SELECT COUNT(*) FROM users WHERE age = 105;";
    let result = process_sql(TEST_DB_LEAF, query)?;
    assert_eq!(result, "1");
    Ok(())
  }

  #[test]
  fn test_interior_page_process_sql_select_columns() -> Result<()> {
    let query = "SELECT username, age FROM users;";
    let result = process_sql(TEST_DB_INTERIOR, &query)?;
    let expected = vec![
      "Alice|29",
      "Bob|45",
      "Charlie|15",
      "Dave|105",
      "Dave2|105",
      "Dave3|105",
      "Dave4|105",
      "Dave5|105",
      "Dave6|105",
      "Dave7|105",
      "Dave8|105",
      "Dave9|105",
      "Dave10|105",
      "Dave11|105",
      "Dave12|105",
      "Dave13|105",
      "Dave14|105",
      "Dave15|105",
      "Dave16|105",
      "Dave17|105",
      "Dave18|105",
      "Celestino|25",
    ]
    .join("\n");
    assert_eq!(result, expected);
    Ok(())
  }

  #[test]
  fn test_interior_page_process_sql_count_function() -> Result<()> {
      let query = "SELECT COUNT(*) FROM users;";
      let result = process_sql(TEST_DB_INTERIOR, query)?;
      assert_eq!(result, "22");
      Ok(())
  }

  #[test]
  fn test_interior_page_process_sql_table_not_found() -> Result<()> {
    let query = "SELECT name FROM non_existent_table;";
    let result = process_sql(TEST_DB_INTERIOR, query);
    assert!(result.is_err());
    assert_eq!(
      result.unwrap_err().to_string(),
      "Table not found: non_existent_table"
    );
    Ok(())
  }

  #[test]
  fn test_interior_page_process_sql_where_clause_equal_integer() -> Result<()> {
    let query = "SELECT username FROM users WHERE age = 25;";
    let result = process_sql(TEST_DB_INTERIOR, query)?;
    let expected = vec!["Celestino"].join("\n");
    assert_eq!(result, expected);
    Ok(())
  }

  #[test]
  fn test_interior_page_process_sql_count_with_where_clause_equal_integer() -> Result<()> {
    let query = "SELECT COUNT(*) FROM users WHERE age = 105;";
    let result = process_sql(TEST_DB_INTERIOR, query)?;
    assert_eq!(result, "18");
    Ok(())
  }

  #[test]
  fn test_process_as_none_if_no_index() -> Result<()> {
    let query = "SELECT id, name FROM users WHERE age = 105;";
    let select_expression = sql::parse_select_sql(query)?;
    let result = process_indices(TEST_DB_INTERIOR, select_expression)?;
    assert_eq!(result, None);
    Ok(())
  }

  #[test]
  fn test_process_index_first_interior_page() -> Result<()> {
    let query = "SELECT id, name FROM users WHERE email = 'dave@example.com';";
    let select_expression = sql::parse_select_sql(query)?;
    let result = process_indices(TEST_DB_INTERIOR, select_expression)?;
    assert_eq!(result, Some(vec![4]));
    Ok(())
  }

  #[test]
  fn test_process_index_last_interior_page() -> Result<()> {
    let query = "SELECT id, name FROM users WHERE email = 'dave18@example.com';";
    let select_expression = sql::parse_select_sql(query)?;
    let result = process_indices(TEST_DB_INTERIOR, select_expression)?;
    assert_eq!(result, Some(vec![21]));
    Ok(())
  }
}