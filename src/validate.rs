use crate::error::*;
use crate::protos::generated::config::*;
use std::collections::HashSet;

fn table_columns_have_unique_names(schema: &TableSchema) -> Result<(), Error> {
    let mut names: HashSet<&String> = HashSet::new();
    for column in schema.columns.iter() {
        if column.name.is_empty() {
            return Err(Error::InvalidArgument(
                "All columns must have a name!".into(),
            ));
        }
        if names.contains(&column.name) {
            return Err(Error::InvalidArgument(
                "Column names are not unique!".into(),
            ));
        }
        names.insert(&column.name);
    }
    Ok(())
}

fn table_columns_have_types(schema: &TableSchema) -> Result<(), Error> {
    for column in schema.columns.iter() {
        if column.column_type.enum_value_or_default() == column_schema::ColumnType::UNDEFINED {
            return Err(Error::InvalidArgument(
                "All columns must have a defined type!".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn schema(schema: &TableSchema) -> Result<(), Error> {
    table_columns_have_types(&schema)?;
    table_columns_have_unique_names(&schema)?;
    Ok(())
}
