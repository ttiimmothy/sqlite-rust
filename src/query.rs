use crate::{
    database::{Database, ObjectSchema},
    record::Value,
};

#[derive(Debug)]
pub enum Query {
    Select(SelectQuery),
    Create(CreateQuery),
}

#[derive(Debug)]
pub struct SelectQuery {
    pub table_name: String,
    pub columns: Vec<Column>,
    pub filter: Option<Filter>,
}

#[derive(Debug, PartialEq)]
pub enum Column {
    Count,
    ColumnName(String),
}

#[derive(Debug)]
pub struct Filter {
    pub column_name: String,
    pub column_value: Value,
}

impl Column {
    #[allow(dead_code)]
    pub fn as_name(&self) -> Option<&str> {
        match self {
            Column::ColumnName(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct CreateQuery {
    pub column_names: Vec<String>,
}

impl Query {
    pub fn parse(query_str: &str) -> anyhow::Result<Self> {
        if query_str.to_ascii_lowercase().starts_with("select") {
            let mut parts = query_str.split_whitespace().peekable();
            assert_eq!(
                parts.next().map(|s| s.to_ascii_lowercase()),
                Some("select".into())
            );

            let mut columns = Vec::new();
            for next_token in parts.by_ref() {
                if next_token == "," || next_token.to_ascii_lowercase() == "from" {
                    break;
                } else if next_token.to_ascii_lowercase().contains("count(") {
                    columns.push(Column::Count);
                } else {
                    let column_name = next_token.trim_start_matches(',').trim_end_matches(',');
                    columns.push(Column::ColumnName(column_name.to_owned()))
                }
            }

            let table_name = parts.next().unwrap().to_ascii_lowercase();

            let mut filter = None;
            if parts.next().map(|s| s.to_ascii_lowercase()) == Some("where".into()) {
                let column_name = parts.next().unwrap().to_ascii_lowercase();
                assert_eq!(parts.next(), Some("="));

                let column_value = if parts.peek().unwrap().starts_with('\'') {
                    // Interpret as text
                    let mut text = String::new();
                    loop {
                        let next_part = parts.next().unwrap();
                        text.push_str(next_part.trim_matches('\''));
                        if next_part.ends_with('\'') {
                            break;
                        }
                        text.push(' ');
                    }
                    Value::Text(text)
                } else {
                    // Interpret as number
                    todo!()
                };

                filter = Some(Filter {
                    column_name,
                    column_value,
                });
            }

            Ok(Query::Select(SelectQuery {
                table_name,
                columns,
                filter,
            }))
        } else if query_str.to_ascii_lowercase().starts_with("create") {
            let (_, columns_info) = query_str.split_once('(').unwrap();
            let columns_info = columns_info.strip_suffix(')').unwrap();
            let columns = columns_info.split(',');

            let mut column_names = Vec::new();
            for column_info in columns {
                let column_name = column_info.split_whitespace().next().unwrap();
                column_names.push(column_name.to_owned());
            }

            Ok(Query::Create(CreateQuery { column_names }))
        } else {
            Err(anyhow::format_err!("unsupported or invalid query type"))
        }
    }

    pub fn as_create(&self) -> Option<&CreateQuery> {
        match self {
            Query::Create(create) => Some(create),
            _ => None,
        }
    }

    pub fn execute<R>(&self, db: &mut Database, mut file: R) -> anyhow::Result<Vec<Vec<String>>>
    where
        R: std::io::Read + std::io::Seek,
    {
        match self {
            Query::Select(select) => {
                if select.columns.iter().any(|c| matches!(c, Column::Count))
                    && select.columns.len() != 1
                {
                    anyhow::bail!("count() queries with more than one select column not supported");
                }

                let table_root_page = db.schema.table_root_page(&select.table_name)?;
                let table_column_names = db
                    .schema
                    .objects
                    .iter()
                    .find(|o| {
                        matches!(o, ObjectSchema::Table(_))
                            && o.as_table().unwrap().root_page == table_root_page
                    })
                    .map(|o| o.as_table().unwrap().column_names.clone())
                    .unwrap();

                let is_count_query = select.columns.iter().any(|c| matches!(c, Column::Count));

                let mut select_column_names = if is_count_query {
                    vec!["id".to_string()]
                } else {
                    select
                        .columns
                        .iter()
                        .map(|c| c.as_name().unwrap().to_owned())
                        .collect::<Vec<_>>()
                };
                if let Some(filter_column_name) =
                    select.filter.as_ref().map(|f| f.column_name.clone())
                {
                    if !select_column_names.contains(&filter_column_name) {
                        select_column_names.push(filter_column_name);
                    }
                }

                let mut column_names = Vec::new();
                let mut column_indices = Vec::new();
                let mut filter_column_index = None;
                for (i, column_name) in table_column_names.iter().enumerate() {
                    if select_column_names.contains(column_name) {
                        column_names.push(column_name.as_str());
                        column_indices.push(i);
                        if let Some(filter_column_name) =
                            select.filter.as_ref().map(|f| f.column_name.clone())
                        {
                            if filter_column_name == *column_name {
                                filter_column_index = Some(column_names.len() - 1);
                            }
                        }
                    }
                }

                // Set to false laster if we find we can use an index instead
                let mut need_to_filter = true;

                let records = if let Some(filter) = select.filter.as_ref() {
                    // See if we can use an index
                    let mut index = None;
                    for object in db.schema.objects.iter() {
                        if let ObjectSchema::Index(idx) = object {
                            if idx.column_name == filter.column_name {
                                index = Some(idx);
                            }
                        }
                    }
                    if let Some(index) = index {
                        let row_ids = db.search_index(
                            &mut file,
                            index.root_page,
                            filter.column_value.clone(),
                        )?;
                        need_to_filter = false;
                        let table_root_page = db.schema.table_root_page(&select.table_name)?;
                        db.get_by_row_ids(
                            file,
                            table_root_page,
                            &row_ids,
                            &column_names,
                            &column_indices,
                        )?
                    } else {
                        // Full table scan
                        db.get_full_table(file, table_root_page, &column_names, &column_indices)?
                    }
                } else {
                    // Full table scan

                    db.get_full_table(file, table_root_page, &column_names, &column_indices)?
                };

                let mut results = Vec::new();
                let mut result_count = 0;
                for record in records.iter() {
                    if need_to_filter {
                        if let Some(filter) = select.filter.as_ref() {
                            let value = &record.values[filter_column_index.unwrap()];
                            if *value != filter.column_value {
                                // Exclude this record from the results
                                continue;
                            }
                        }
                    }

                    result_count += 1;

                    if !is_count_query {
                        let mut row = Vec::new();
                        for column in select.columns.iter() {
                            #[allow(clippy::single_match)]
                            match column {
                                Column::ColumnName(column_name) => {
                                    let record_index =
                                        column_names.iter().position(|c| c == column_name).unwrap();
                                    row.push(record.values[record_index].to_string());
                                }
                                _ => {}
                            }
                        }
                        results.push(row);
                    }
                }

                if is_count_query {
                    results.push(vec![result_count.to_string()]);
                }

                Ok(results)
            }
            _ => todo!("non select query"),
        }
    }
}
