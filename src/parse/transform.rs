#[cfg(test)]
#[path = "./transform_test.rs"]
mod test;

use crate::protos::generated::operations::*;
use crate::protos::generated::config::*;
use crate::protos::generated::chunk::*;

fn get_col_value_as_string(col_val: &ColumnValueProto) -> String {
    match col_val.value {
        Some(column_value_proto::Value::IntValue(i)) => i.to_string(),
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
pub fn insert_op(validated_op: InsertProto, schema: &IndexSchema) -> InternalRowProto {
    let mut internal_row = InternalRowProto::new();
    for col_val in &validated_op.column_values {
        let col_schema: &ColumnSchema = get_col_schema(schema, &col_val.name);
        if col_schema.is_key {
            internal_row.key += &get_col_value_as_string(col_val);
            internal_row.key += ".";
        }
        let mut internal_col = InternalColumnProto::new();
        match col_val.value {
            Some(column_value_proto::Value::IntValue(i)) => internal_col.set_int_value(i),
            None => unreachable!(),
        };
        internal_row.col_values.push(internal_col);
    }
    internal_row
}

// TODO: validate all keys present
// TODO: validate all cols have values
pub fn read_row_op(validated_op: ReadRowProto, schema: &IndexSchema) -> String {
    let mut key = String::new();
    for key_val in &validated_op.key_values {
        key += &get_col_value_as_string(key_val);
        key += ".";
    }
    key
}
