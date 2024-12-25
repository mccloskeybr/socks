use crate::chunk;
use crate::database;
use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::schema;
use crate::table;
use crate::table::Table;
use protobuf::text_format::parse_from_str;
use protobuf::MessageField;
use std::cell::RefCell;
use std::io::Cursor;
use std::rc::Rc;

struct TestContext {
    config: TableConfig,
    schema: DatabaseSchema,
}

fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    TestContext {
        schema: parse_from_str::<DatabaseSchema>(
            "
            table {
                key {
                    name: \"Key\"
                    column_type: INTEGER
                }
                columns {
                    name: \"Value\"
                    column_type: INTEGER
                }
            }
            secondary_indexes {
                key {
                    name: \"Value\"
                    column_type: INTEGER
                }
            }
            ",
        )
        .unwrap(),
        config: parse_from_str::<TableConfig>(
            "
            chunk_size: 512
            chunk_overflow_size: 10
            ",
        )
        .unwrap(),
    }
}

#[test]
fn insert_single_success() -> Result<(), Error> {
    let mut context = setup();
    let mut db = database::create::<Cursor<Vec<u8>>>("", context.config, context.schema)?;

    let insert_operation = parse_from_str::<InsertProto>(
        "
        row {
            columns {
                name: \"Key\"
                value {
                    int_value: 1
                }
            }
            columns {
                name: \"Value\"
                value {
                    int_value: 2
                }
            }
        }
        ",
    )?;
    database::insert(&mut db, insert_operation.clone())?;

    // row in primary index
    {
        let expected_table_row_internal = insert_operation.row.clone().unwrap();
        let table_row_internal = table::read_row(&mut db.table.borrow_mut(), 1)?;
        assert_eq!(expected_table_row_internal, table_row_internal);
    }
    // row in secondary index
    {
        let secondary_index = &mut db.secondary_indexes[0];
        let index_row = schema::table_row_to_index_row(
            &insert_operation.row,
            &secondary_index.borrow().metadata.schema.as_ref().unwrap(),
            &db.table.borrow().metadata.schema.as_ref().unwrap(),
        );
        let expected_index_row_internal = index_row;
        let index_row_internal = table::read_row(&mut secondary_index.borrow_mut(), 2)?;
        assert_eq!(expected_index_row_internal, index_row_internal);
    }

    Ok(())
}

#[test]
fn read_single_success() -> Result<(), Error> {
    let mut context = setup();
    let mut db = database::create::<Cursor<Vec<u8>>>("", context.config, context.schema)?;

    let insert_operation = parse_from_str::<InsertProto>(
        "
        row {
            columns {
                name: \"Key\"
                value {
                    int_value: 1
                }
            }
            columns {
                name: \"Value\"
                value {
                    int_value: 2
                }
            }
        }
        ",
    )?;
    database::insert(&mut db, insert_operation.clone())?;

    let read_operation = parse_from_str::<ReadRowProto>(
        "
        column {
            name: \"Key\"
            value {
                int_value: 1
            }
        }
        ",
    )?;
    let row = database::read_row(&mut db, read_operation);

    assert_eq!(insert_operation.row.unwrap(), row.unwrap());

    Ok(())
}

#[test]
fn query_success() -> Result<(), Error> {
    let mut context = setup();
    let mut db = database::create::<Cursor<Vec<u8>>>("", context.config, context.schema)?;

    for i in 0..50 {
        let mut key = ColumnProto::new();
        key.name = "Key".to_string();
        key.value.mut_or_insert_default().set_int_value(i);
        let mut val = ColumnProto::new();
        val.name = "Value".to_string();
        val.value.mut_or_insert_default().set_int_value(i * 10);

        let mut row = RowProto::new();
        row.columns.push(key);
        row.columns.push(val);

        let mut insert_operation = InsertProto::new();
        insert_operation.row = MessageField::some(row);
        database::insert(&mut db, insert_operation)?;
    }

    let query_operation = parse_from_str::<QueryProto>(
        "
        select {
            dep {
                filter {
                    equals {
                        name: \"Value\"
                        value {
                            int_value: 250
                        }
                    }
                }
            }
        }
        ",
    )?;
    let mut query_results_file = database::query(&mut db, query_operation)?;
    let mut query_results: InternalQueryResultsProto = chunk::read_chunk_at(
        &db.table.borrow().metadata.config.clone().unwrap(),
        &mut query_results_file,
        0,
    )?;

    let expected_query_results = parse_from_str::<InternalQueryResultsProto>(
        "
        keys: 25
        rows {
            columns {
                name: \"Key\"
                value {
                    int_value: 25
                }
            }
            columns {
                name: \"Value\"
                value {
                    int_value: 250
                }
            }
        }
        ",
    )?;
    assert_eq!(query_results, expected_query_results);

    Ok(())
}
