use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;

fn get_index_key_value(index_schema: &TableSchema, row: &RowProto) -> ColumnProto {
    for col in &row.columns {
        let col_schema = get_col_schema(index_schema, &col.name);
        if let Some(col_schema) = col_schema {
            if col_schema.is_key {
                return col.clone();
            }
        };
    }
    todo!();
}

pub(crate) fn get_hashed_col_value(col_val: &ColumnProto) -> u32 {
    match col_val.value {
        Some(column_proto::Value::IntValue(i)) => i as u32,
        Some(column_proto::Value::UintValue(u)) => u,
        None => unreachable!(),
    }
}

pub(crate) fn get_primary_key<'a>(schema: &'a TableSchema) -> &'a ColumnSchema {
    for col in &schema.columns {
        if col.is_key {
            return col;
        }
    }
    todo!();
}

pub(crate) fn get_col_schema<'a>(
    schema: &'a TableSchema,
    col_name: &String,
) -> Option<&'a ColumnSchema> {
    for col in &schema.columns {
        if col.name == *col_name {
            return Some(col);
        }
    }
    None
}

pub(crate) fn get_hashed_key_from_row(row: &RowProto, schema: &TableSchema) -> u32 {
    for col in &row.columns {
        let col_schema = get_col_schema(schema, &col.name);
        if let Some(col_schema) = col_schema {
            if col_schema.is_key {
                return get_hashed_col_value(col);
            }
        };
    }
    todo!();
}

// TODO: this doesn't do any validations currently.
pub(crate) fn row_to_internal_row(row: &RowProto, schema: &TableSchema) -> InternalRowProto {
    let mut key = 0;
    let mut internal_row = InternalRowProto::new();
    for col in &row.columns {
        let mut internal_col = InternalColumnProto::new();
        match col.value {
            Some(column_proto::Value::IntValue(i)) => internal_col.set_int_value(i),
            Some(column_proto::Value::UintValue(u)) => internal_col.set_uint_value(u),
            None => unreachable!(),
        };
        internal_row.col_values.push(internal_col);
    }
    internal_row
}

pub(crate) fn internal_row_to_row(
    internal_row: &InternalRowProto,
    schema: &TableSchema,
) -> RowProto {
    let columns = internal_row
        .col_values
        .iter()
        .zip(schema.columns.iter())
        .map(|(internal_column, column_schema)| {
            let mut column = ColumnProto::new();
            column.name = column_schema.name.clone();
            match internal_column.column_type {
                None => {}
                Some(internal_column_proto::Column_type::IntValue(i)) => column.set_int_value(i),
                Some(internal_column_proto::Column_type::UintValue(u)) => column.set_uint_value(u),
            }
            column
        })
        .collect();

    let mut row = RowProto::new();
    row.columns = columns;

    row
}

pub(crate) fn create_table_schema_for_index(
    index_schema: &IndexSchema,
    table_schema: &TableSchema,
) -> TableSchema {
    let mut index_key = index_schema.column.clone();

    let mut table_key = ColumnSchema::new();
    table_key.name = "PrimaryKeyHash".to_string();
    table_key.type_ = column_schema::Type::UNSIGNED_INTEGER.into();

    let mut table_schema = TableSchema::new();
    table_schema.columns.push(index_key.unwrap());
    table_schema.columns.push(table_key);

    table_schema
}

pub(crate) fn table_row_to_index_row(
    row: &RowProto,
    index_schema: &TableSchema,
    primary_key_hash: u32,
) -> RowProto {
    let index_key = get_index_key_value(index_schema, row);
    let mut table_key_hash = ColumnProto::new();
    table_key_hash.name = "PrimaryKeyHash".to_string();
    table_key_hash.set_uint_value(primary_key_hash);

    let mut index_row = RowProto::new();
    index_row.columns = vec![index_key, table_key_hash];

    index_row
}
