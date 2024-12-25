use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use protobuf::MessageField;
use std::iter;

pub(crate) fn get_col<'a>(row: &'a RowProto, col_name: &str) -> &'a ColumnProto {
    for col in &row.columns {
        if col.name == col_name {
            return col;
        }
    }
    todo!();
}

pub(crate) fn get_hashed_key_from_row(row: &RowProto, schema: &TableSchema) -> u32 {
    let key_column = get_col(row, &schema.key.name);
    get_hashed_col_value(key_column.value.as_ref().unwrap())
}

pub(crate) fn get_hashed_col_value(value: &ValueProto) -> u32 {
    match value.value_type {
        Some(value_proto::Value_type::IntValue(i)) => i as u32,
        Some(value_proto::Value_type::UintValue(u)) => u,
        None => unreachable!(),
    }
}

pub(crate) fn internal_col_to_col(value: &ValueProto, column_schema: &ColumnSchema) -> ColumnProto {
    let mut column = ColumnProto::new();
    column.name = column_schema.name.clone();
    column.value = MessageField::some(value.clone());
    column
}

// TODO: this doesn't do any validations currently.
pub(crate) fn row_to_internal_row(row: &RowProto, schema: &TableSchema) -> InternalRowProto {
    let mut internal_row = InternalRowProto::new();
    for col in &row.columns {
        internal_row.col_values.push(col.value.clone().unwrap());
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
        .zip(iter::once(schema.key.as_ref().unwrap()).chain(schema.columns.iter()))
        .map(|(internal_column, column_schema)| internal_col_to_col(internal_column, column_schema))
        .collect();

    let mut row = RowProto::new();
    row.columns = columns;

    row
}

pub(crate) fn create_table_schema_for_index(
    index_schema: &IndexSchema,
    table_schema: &TableSchema,
) -> TableSchema {
    let mut table_key = table_schema.key.clone().unwrap();

    let mut index_table_schema = TableSchema::new();
    index_table_schema.key = index_schema.key.clone();
    index_table_schema.columns.push(table_key);

    index_table_schema
}

pub(crate) fn table_row_to_index_row(
    row: &RowProto,
    index_schema: &TableSchema,
    table_schema: &TableSchema,
) -> RowProto {
    let index_key = get_col(row, &index_schema.key.name);
    let mut table_key = get_col(row, &table_schema.key.as_ref().unwrap().name);

    let mut index_row = RowProto::new();
    index_row.columns = vec![index_key.clone(), table_key.clone()];

    index_row
}
