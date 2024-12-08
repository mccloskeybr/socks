#[cfg(test)]
#[path = "./transform_test.rs"]
mod test;

use std::collections::hash_map::DefaultHasher;
use crate::protos::generated::operations::*;
use crate::protos::generated::config::*;
use crate::protos::generated::chunk::*;

fn get_col_value_as_u32(col_val: &ColumnValueProto) -> u32 {
    match col_val.value {
        Some(column_value_proto::Value::IntValue(i)) => i as u32,
        None => unreachable!(),
    }
}

fn get_col_schema<'a>(schema: &'a IndexSchema, col_name: &String) -> &'a ColumnSchema {
    for col in &schema.columns {
        if col.name == *col_name {
            return col;
        }
    }
    unreachable!();
}

// TODO: validate no duplicate columns
// TODO: validate keys specified first
// TODO: validate column value match b/w op and schema
pub fn insert_op(validated_op: InsertProto, schema: &IndexSchema)
-> (u32, InternalRowProto) {
    let mut key = 0;
    let mut row = InternalRowProto::new();
    for col_val in &validated_op.column_values {
        let col_schema: &ColumnSchema = get_col_schema(schema, &col_val.name);
        if col_schema.is_key {
            key = get_col_value_as_u32(col_val);
        }
        let mut internal_col = InternalColumnProto::new();
        match col_val.value {
            Some(column_value_proto::Value::IntValue(i)) => internal_col.set_int_value(i),
            None => unreachable!(),
        };
        row.col_values.push(internal_col);
    }
    (key, row)
}

// TODO: validate all keys present
// TODO: validate all cols have values
pub fn read_row_op(validated_op: ReadRowProto, schema: &IndexSchema) -> u32 {
    get_col_value_as_u32(&validated_op.key_value)
}
