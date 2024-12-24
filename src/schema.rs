use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
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
    get_hashed_col_value(key_column)
}

pub(crate) fn get_hashed_col_value(col_val: &ColumnProto) -> u32 {
    match col_val.value {
        Some(column_proto::Value::IntValue(i)) => i as u32,
        Some(column_proto::Value::UintValue(u)) => u,
        None => unreachable!(),
    }
}

pub(crate) fn col_to_internal_col(col: &ColumnProto) -> InternalColumnProto {
    let mut internal_col = InternalColumnProto::new();
    match col.value {
        Some(column_proto::Value::IntValue(i)) => internal_col.set_int_value(i),
        Some(column_proto::Value::UintValue(u)) => internal_col.set_uint_value(u),
        None => unreachable!(),
    };
    internal_col
}

pub(crate) fn internal_col_to_col(
    internal_column: &InternalColumnProto,
    column_schema: &ColumnSchema,
) -> ColumnProto {
    let mut column = ColumnProto::new();
    column.name = column_schema.name.clone();
    match internal_column.column_type {
        None => {}
        Some(internal_column_proto::Column_type::IntValue(i)) => column.set_int_value(i),
        Some(internal_column_proto::Column_type::UintValue(u)) => column.set_uint_value(u),
    }
    column
}

// TODO: this doesn't do any validations currently.
pub(crate) fn row_to_internal_row(row: &RowProto, schema: &TableSchema) -> InternalRowProto {
    let mut internal_row = InternalRowProto::new();
    for col in &row.columns {
        internal_row.col_values.push(col_to_internal_col(col));
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

pub(crate) fn is_schema_keyed_on_column(schema: &TableSchema, col: &ColumnProto) -> bool {
    schema.key.name == col.name
}
