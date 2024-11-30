#[cfg(test)]
#[path = "./transform_test.rs"]
mod test;

use crate::error::*;
use crate::protos::generated::operations::*;
use crate::protos::generated::schema::*;
use crate::protos::generated::chunk::*;

fn get_col_value_as_string(col_val: &ColumnValue) -> String {
    match col_val.value {
        Some(column_value::Value::IntValue(i)) => i.to_string(),
        None => unreachable!(),
    }
}

fn get_col_schema<'a>(schema: &'a IndexSchema, col_name: &String)
-> &'a ColumnSchema {
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
pub fn transform_insert_op(validated_op: Insert, schema: &IndexSchema)
-> InternalRow {
    let mut internal_row = InternalRow::new();
    for col_val in &validated_op.column_values {
        let col_schema: &ColumnSchema = get_col_schema(schema, &col_val.name);
        if col_schema.is_key {
            internal_row.key += &get_col_value_as_string(col_val);
            internal_row.key += ".";
        }
        let mut internal_col = InternalColumn::new();
        match col_val.value {
            Some(column_value::Value::IntValue(i)) => internal_col.set_int_value(i),
            None => unreachable!(),
        };
        internal_row.col_values.push(internal_col);
    }
    internal_row
}
