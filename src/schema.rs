use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;

fn get_index_key_value(
    index_schema: &TableSchema,
    cols: &Vec<ColumnValueProto>,
) -> ColumnValueProto {
    for col in cols {
        let col_schema = get_col_schema(index_schema, &col.name);
        if let Some(col_schema) = col_schema {
            if col_schema.is_key {
                return col.clone();
            }
        };
    }
    todo!();
}

pub(crate) fn get_col_value_as_u32(col_val: &ColumnValueProto) -> u32 {
    match col_val.value {
        Some(column_value_proto::Value::IntValue(i)) => i as u32,
        Some(column_value_proto::Value::UintValue(u)) => u,
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

pub(crate) fn get_hashed_key(schema: &TableSchema, cols: &Vec<ColumnValueProto>) -> u32 {
    for col in cols {
        let col_schema = get_col_schema(schema, &col.name);
        if let Some(col_schema) = col_schema {
            if col_schema.is_key {
                return get_col_value_as_u32(col);
            }
        };
    }
    todo!();
}

// TODO: this doesn't do any validations currently.
pub(crate) fn to_row(schema: &TableSchema, cols: &Vec<ColumnValueProto>) -> InternalRowProto {
    let mut key = 0;
    let mut row = InternalRowProto::new();
    for col in cols {
        let mut internal_col = InternalColumnProto::new();
        match col.value {
            Some(column_value_proto::Value::IntValue(i)) => internal_col.set_int_value(i),
            Some(column_value_proto::Value::UintValue(u)) => internal_col.set_uint_value(u),
            None => unreachable!(),
        };
        row.col_values.push(internal_col);
    }
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

pub(crate) fn create_index_cols(
    index_schema: &TableSchema,
    cols: &Vec<ColumnValueProto>,
    primary_key_hash: u32,
) -> Vec<ColumnValueProto> {
    let index_key = get_index_key_value(index_schema, cols);
    let mut table_key_hash = ColumnValueProto::new();
    table_key_hash.name = "PrimaryKeyHash".to_string();
    table_key_hash.set_uint_value(primary_key_hash);

    vec![index_key, table_key_hash]
}
