use anyhow::Result;
peg::parser! {
  grammar sql_parser() for str {
    pub rule create_table_statement() -> CreateTableStatement = kw("CREATE") ws() kw("TABLE") ws() if_not_exists:optional_if_not_exists()? table_name:identifier() ws() "(" wsz() columns:column_definition() ** (wsz() "," wsz()) wsz() ")" ";"? {
      CreateTableStatement {
        table_name,
        columns,
        if_not_exists: if_not_exists.is_some(),
      }
    }

    pub rule create_index_statement() -> CreateIndexStatement = kw("CREATE") ws() unique:(kw("UNIQUE") ws())? kw("INDEX") ws() if_not_exists:optional_if_not_exists()? index_name:identifier() wsz() kw("ON") ws() table_name:identifier() wsz() "(" wsz() columns:column_list() ")" ";"? {
      CreateIndexStatement {
        index_name,
        table_name,
        columns,
        unique: unique.is_some(),
        if_not_exists: if_not_exists.is_some(),
      }
    }

    pub rule select_expression() -> SelectExpression = kw("SELECT") ws() select_clause:select_clause() ws() kw("FROM") ws() table:identifier() where_clause:optional_where_clause()? ";"? {
      SelectExpression {
        select_clause,
        table,
        where_clause,
      }
    }

    rule select_clause() -> SelectClause = call:function_call() { call }
      / columns:column_list() { SelectClause::Columns(columns) }
    rule function_call() -> SelectClause = name:identifier() "(*)" { SelectClause::FunctionCall { name: name.to_owned().to_uppercase(), args: vec!["*".to_owned()]} }
    rule column_list() -> Vec<String> = column: (identifier() ** ("," wsz())) { column }
    rule column_definition() -> ColumnDefinition = name:identifier() wsz()
      data_type:data_type()
      wsz()
      constraints:column_constraints() {
        ColumnDefinition {
          name,
          data_type,
          constraints
        }
      }
    rule column_constraints() -> Vec<ColumnConstraint> = constraints:column_constraint() ** (wsz()) { constraints }
    rule column_constraint() -> ColumnConstraint =
      kw("PRIMARY") ws() kw("KEY") ws()? autoincrement:kw("AUTOINCREMENT")? {
        ColumnConstraint::PrimaryKey { auto_increment: autoincrement.is_some() }
      }
      / kw("NOT") ws() kw("NULL") { ColumnConstraint::NotNull }
      / kw("UNIQUE") { ColumnConstraint::Unique }
      / kw("DEFAULT") ws() value:default_value() { ColumnConstraint::Default(value) }
    rule default_value() -> String = value:quoted_string() { value }
      / value:$(['0'..='9']+) { value.to_owned() }
      / kw("CURRENT_TIMESTAMP") { "CURRENT_TIMESTAMP".to_owned() }
    rule data_type() -> DataType = kw("INTEGER") { DataType::Integer }
      / kw("TEXT") { DataType::Text }
      / kw("TIMESTAMP") { DataType::Timestamp }
    rule optional_where_clause() -> WhereClause =  ws() kw("WHERE") ws()
      column:identifier() wsz()
      operator:operator() wsz()
      value:where_value() {
        WhereClause {
          column,
          operator,
          value,
        }
      }
    rule optional_if_not_exists() -> () = wsz() kw("IF") ws() kw("NOT") ws() kw("EXISTS") ws()
    rule operator() -> Operator = "=" { Operator::Equal }
      / "!=" { Operator::NotEqual }
      / "<" { Operator::LessThan }
      / "<=" { Operator::LessThanOrEqual }
      / ">" { Operator::GreaterThan }
      / ">=" { Operator::GreaterThanOrEqual }
    rule where_value() -> WhereValue = quote:quoted_string() { WhereValue::String(quote.to_owned()) }
      / number:$(['0'..='9']+) { WhereValue::Number(number.to_owned().parse().unwrap()) }
    rule quoted_string() -> String = "'" value:$([^'\'']*) "'" { value.to_owned() }
    rule identifier() -> String = s:$(['a'..='z' | 'A'..='Z' | '_']+) { s.to_owned() }
      / "\"" s:$(['a'..='z' | 'A'..='Z' | '_' | ' ']+) "\"" { s.to_owned() }
    rule ws() = quiet!{[' ' | '\n' | '\t']+}
    rule wsz() = quiet!{[' ' | '\n' | '\t']*}
    rule kw(kw: &'static str) -> () = input:$([_]*<{kw.len()}>) {? if input.eq_ignore_ascii_case(kw) { Ok(()) } else { Err(kw) } }
  }
}

#[derive(Debug, PartialEq)]
pub struct CreateTableStatement {
  pub table_name: String,
  pub columns: Vec<ColumnDefinition>,
  pub if_not_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct CreateIndexStatement {
  pub index_name: String,
  pub table_name: String,
  pub columns: Vec<String>,
  pub unique: bool,
  pub if_not_exists: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnDefinition {
  pub name: String,
  pub data_type: DataType,
  pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
  Integer,
  Text,
  Timestamp,
}

// For more info, see https://www.sqlite.org/syntax/column-constraint.html
#[derive(Debug, PartialEq, Clone)]
pub enum ColumnConstraint {
  PrimaryKey { auto_increment: bool },
  NotNull,
  Unique,
  Default(String),
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectExpression {
  pub select_clause: SelectClause,
  pub table: String,
  pub where_clause: Option<WhereClause>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SelectClause {
  Columns(Vec<String>),
  FunctionCall { name: String, args: Vec<String> },
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhereClause {
  pub column: String,
  pub operator: Operator,
  pub value: WhereValue,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum WhereValue {
  String(String),
  Number(String),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
  Equal,
  NotEqual,
  LessThan,
  LessThanOrEqual,
  GreaterThan,
  GreaterThanOrEqual,
}
pub fn parse_select_sql(input: &str) -> Result<SelectExpression> {
  sql_parser::select_expression(input).map_err(|e| anyhow::anyhow!("{}", e))
}
pub fn parse_create_table_sql(input: &str) -> Result<CreateTableStatement> {
  sql_parser::create_table_statement(input).map_err(|e| anyhow::anyhow!("{}", e))
}
pub fn parse_create_index_sql(input: &str) -> Result<CreateIndexStatement> {
  sql_parser::create_index_statement(input).map_err(|e| anyhow::anyhow!("{}", e))
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn test_parse_select_sql_with_columns() {
    let sql = "SELECT id, name FROM users";
    let expected = SelectExpression {
      select_clause: SelectClause::Columns(vec!["id".to_owned(), "name".to_owned()]),
      table: "users".to_owned(),
      where_clause: None,
    };
    assert_eq!(parse_select_sql(sql).unwrap(), expected);
  }

  #[test]
  fn test_parse_select_sql_with_function_call() {
    let sql = "SELECT COUNT(*) FROM users";
    let expected = SelectExpression {
      select_clause: SelectClause::FunctionCall {
        name: "COUNT".to_owned(),
        args: vec!["*".to_owned()],
      },
      table: "users".to_owned(),
      where_clause: None,
    };
    assert_eq!(parse_select_sql(sql).unwrap(), expected);
  }

  #[test]
  fn test_parse_select_sql_with_where_clause() {
    let sql = "SELECT id FROM users WHERE name = 'Alice'";
    let expected = SelectExpression {
      select_clause: SelectClause::Columns(vec!["id".to_owned()]),
      table: "users".to_owned(),
      where_clause: Some(WhereClause {
        column: "name".to_owned(),
        operator: Operator::Equal,
        value: WhereValue::String("Alice".to_owned()),
      }),
    };
    assert_eq!(parse_select_sql(sql).unwrap(), expected);
  }

  #[test]
  fn test_parse_select_sql_with_invalid_syntax() {
    let sql = "SELECT FROM users";
    assert!(parse_select_sql(sql).is_err());
  }

  #[test]
  fn test_parse_select_sql_with_invalid_where_clause() {
    let sql = "SELECT id FROM users WHERE name 'Alice'";
    assert!(parse_select_sql(sql).is_err());
  }

  #[test]
  fn test_create_table_statement_type_only_single_line() -> Result<()> {
    let sql = "CREATE TABLE apples (id INTEGER, name TEXT, color TEXT)";
    let expected = CreateTableStatement {
      table_name: "apples".to_owned(),
      columns: vec![
        ColumnDefinition {
          name: "id".to_owned(),
          data_type: DataType::Integer,
          constraints: vec![],
        },
        ColumnDefinition {
          name: "name".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
        ColumnDefinition {
          name: "color".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
      ],
      if_not_exists: false,
    };
    assert_eq!(parse_create_table_sql(sql)?, expected);
    Ok(())
  }

  #[test]
  fn test_create_table_statement_type_only_multi_line() -> Result<()> {
    let sql = "CREATE TABLE apples (id INTEGER, name TEXT, color TEXT)";
    let expected = CreateTableStatement {
      table_name: "apples".to_owned(),
      columns: vec![
        ColumnDefinition {
          name: "id".to_owned(),
          data_type: DataType::Integer,
          constraints: vec![],
        },
        ColumnDefinition {
          name: "name".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
        ColumnDefinition {
          name: "color".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
      ],
      if_not_exists: false,
    };
    assert_eq!(parse_create_table_sql(sql)?, expected);
    Ok(())
  }

  #[test]
  fn test_create_table_statement_type_with_constraints() -> Result<()> {
    let sql = "CREATE TABLE apples (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, color TEXT)";
    let expected = CreateTableStatement {
      table_name: "apples".to_owned(),
      columns: vec![
        ColumnDefinition {
          name: "id".to_owned(),
          data_type: DataType::Integer,
          constraints: vec![ColumnConstraint::PrimaryKey {
            auto_increment: true,
          }],
        },
        ColumnDefinition {
          name: "name".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
        ColumnDefinition {
          name: "color".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
      ],
      if_not_exists: false,
    };
    assert_eq!(parse_create_table_sql(sql)?, expected);
    Ok(())
  }

  #[test]
  fn test_superheroes() -> Result<()> {
    let sql = "CREATE TABLE IF NOT EXISTS \"superheroes\" (id integer primary key autoincrement, name text not null, eye_color text, hair_color text, appearance_count integer, first_appearance text, first_appearance_year text);";
    let expected = CreateTableStatement {
      table_name: "superheroes".to_owned(),
      columns: vec![
        ColumnDefinition {
          name: "id".to_owned(),
          data_type: DataType::Integer,
          constraints: vec![ColumnConstraint::PrimaryKey {
            auto_increment: true,
          }],
        },
        ColumnDefinition {
          name: "name".to_owned(),
          data_type: DataType::Text,
          constraints: vec![ColumnConstraint::NotNull],
        },
        ColumnDefinition {
          name: "eye_color".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
        ColumnDefinition {
          name: "hair_color".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
        ColumnDefinition {
          name: "appearance_count".to_owned(),
          data_type: DataType::Integer,
          constraints: vec![],
        },
        ColumnDefinition {
          name: "first_appearance".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
        ColumnDefinition {
          name: "first_appearance_year".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
      ],
      if_not_exists: true,
    };
    assert_eq!(parse_create_table_sql(sql)?, expected);
    Ok(())
  }

  #[test]
  fn test_companies() -> Result<()> {
    let sql = "CREATE TABLE companies (id integer primary key autoincrement, name text, \"size range\" text)";
    let expected = CreateTableStatement {
      table_name: "companies".to_owned(),
      columns: vec![
        ColumnDefinition {
          name: "id".to_owned(),
          data_type: DataType::Integer,
          constraints: vec![ColumnConstraint::PrimaryKey {
            auto_increment: true,
          }],
        },
        ColumnDefinition {
          name: "name".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
        ColumnDefinition {
          name: "size range".to_owned(),
          data_type: DataType::Text,
          constraints: vec![],
        },
      ],
      if_not_exists: false,
    };
    assert_eq!(parse_create_table_sql(sql)?, expected);
    Ok(())
  }

  #[test]
  fn test_eye_color() -> Result<()> {
    let sql = "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'";
    let expected = SelectExpression {
      select_clause: SelectClause::Columns(vec!["id".to_owned(), "name".to_owned()]),
      table: "superheroes".to_owned(),
      where_clause: Some(WhereClause {
        column: "eye_color".to_owned(),
        operator: Operator::Equal,
        value: WhereValue::String("Pink Eyes".to_owned()),
      }),
    };
    assert_eq!(parse_select_sql(sql)?, expected);
    Ok(())
  }

  #[test]
  fn test_create_index_1() -> Result<()> {
    let sql = "CREATE UNIQUE INDEX idx_users_email ON users (email);";
    let expected = CreateIndexStatement {
      index_name: "idx_users_email".to_owned(),
      table_name: "users".to_owned(),
      columns: vec!["email".to_owned()],
      unique: true,
      if_not_exists: false,
    };
    assert_eq!(parse_create_index_sql(sql)?, expected);
    Ok(())
  }

  #[test]
  fn test_create_index_2() -> Result<()> {
    let sql = "CREATE INDEX idx_companies_country on companies (country)";
    let expected = CreateIndexStatement {
      index_name: "idx_companies_country".to_owned(),
      table_name: "companies".to_owned(),
      columns: vec!["country".to_owned()],
      unique: false,
      if_not_exists: false,
    };
    assert_eq!(parse_create_index_sql(sql)?, expected);
    Ok(())
  }
}