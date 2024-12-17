use crate::database::*;
use crate::error::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::table::table;
use crate::table::table::Table;
use protobuf::text_format::parse_from_str;
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
                columns {
                    name: \"Key\"
                    type: INTEGER
                    is_key: true
                }
                columns {
                    name: \"Value\"
                    type: INTEGER
                }
            }
            secondary_indexes {
                column {
                    name: \"Value\"
                    type: INTEGER
                    is_key: true
                }
            }
            ",
        )
        .unwrap(),
        config: parse_from_str::<TableConfig>(
            "
            insert_method: AGGRESSIVE_SPLIT
            read_method: BINARY_SEARCH
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
    let mut db = create::<Cursor<Vec<u8>>>("", context.config, context.schema)?;

    let insert_operation = parse_from_str::<InsertProto>(
        "
        row {
            columns {
                name: \"Key\"
                int_value: 1
            }
            columns {
                name: \"Value\"
                int_value: 2
            }
        }
        ",
    )?;
    insert(&mut db, insert_operation.clone())?;

    // row in primary index
    {
        let expected_table_row_internal =
            schema::row_to_internal_row(&insert_operation.row, &db.table.borrow().metadata.schema);
        let table_row_internal = table::read_row(&mut db.table.borrow_mut(), 1)?;
        assert_eq!(expected_table_row_internal, table_row_internal);
    }
    // row in secondary index
    {
        let secondary_index = &mut db.secondary_indexes[0];
        let index_row = schema::table_row_to_index_row(
            &insert_operation.row,
            &secondary_index.borrow().metadata.schema.as_ref().unwrap(),
            1,
        );
        let expected_index_row_internal =
            schema::row_to_internal_row(&index_row, &secondary_index.borrow().metadata.schema);
        let index_row_internal = table::read_row(&mut secondary_index.borrow_mut(), 2)?;
        assert_eq!(expected_index_row_internal, index_row_internal);
    }

    Ok(())
}

#[test]
fn read_single_success() -> Result<(), Error> {
    let mut context = setup();
    let mut db = create::<Cursor<Vec<u8>>>("", context.config, context.schema)?;

    let insert_operation = parse_from_str::<InsertProto>(
        "
        row {
            columns {
                name: \"Key\"
                int_value: 1
            }
            columns {
                name: \"Value\"
                int_value: 2
            }
        }
        ",
    )?;
    insert(&mut db, insert_operation.clone())?;

    let read_operation = parse_from_str::<ReadRowProto>(
        "
        key_value {
            name: \"Key\"
            int_value: 1
        }
        ",
    )?;
    let row = read_row(&mut db, read_operation);

    assert_eq!(insert_operation.row.unwrap(), row.unwrap());

    Ok(())
}
