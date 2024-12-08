use crate::error::*;
use crate::protos::generated::config::*;
use std::collections::HashSet;

fn table_has_name(schema: &IndexSchema) -> Result<(), Error> {
    if schema.name.is_empty() {
        return Err(Error::InvalidArgument(
            "Table must be defined with a name!".into(),
        ));
    }
    Ok(())
}

fn table_columns_have_unique_names(schema: &IndexSchema) -> Result<(), Error> {
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

fn table_columns_have_types(schema: &IndexSchema) -> Result<(), Error> {
    for column in schema.columns.iter() {
        if column.type_.enum_value_or_default() == column_schema::Type::UNDEFINED {
            return Err(Error::InvalidArgument(
                "All columns must have a defined type!".into(),
            ));
        }
    }
    Ok(())
}

fn table_has_single_primary_key(schema: &IndexSchema) -> Result<(), Error> {
    let mut primary_key_seen = false;
    for column in schema.columns.iter() {
        if column.is_key {
            if primary_key_seen {
                return Err(Error::InvalidArgument(
                    "Multiple primary keys defined!".into(),
                ));
            }
            primary_key_seen = true;
        }
    }
    if primary_key_seen == false {
        return Err(Error::InvalidArgument(
            "A single primary key must be defined per table!".into(),
        ));
    }
    Ok(())
}

pub fn schema(schema: &IndexSchema) -> Result<(), Error> {
    table_has_name(&schema)?;
    table_has_single_primary_key(&schema)?;
    table_columns_have_types(&schema)?;
    table_columns_have_unique_names(&schema)?;
    Ok(())
}
