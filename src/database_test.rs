use crate::database::*;
use crate::error::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::table::table;
use protobuf::text_format::parse_from_str;
use std::io::Cursor;

pub fn create_for_test(
    config: TableConfig,
    schema: DatabaseSchema,
) -> Result<Database<Cursor<Vec<u8>>>, Error> {
    let table = table::create(
        Cursor::<Vec<u8>>::new(Vec::new()),
        config.clone(),
        schema.table.clone().unwrap(),
    )?;

    let mut secondary_indexes = Vec::<table::Table<Cursor<Vec<u8>>>>::new();
    for secondary_index_schema in schema.secondary_indexes {
        secondary_indexes.push(table::create(
            Cursor::<Vec<u8>>::new(Vec::new()),
            config.clone(),
            schema::create_table_schema_for_index(
                &secondary_index_schema,
                &schema.table.as_ref().unwrap(),
            ),
        )?);
    }

    Ok(Database {
        table: table,
        secondary_indexes: secondary_indexes,
    })
}

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
    let mut db = create_for_test(context.config, context.schema)?;

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
            schema::row_to_internal_row(&insert_operation.row, &db.table.metadata.schema);
        let table_row_internal = table::read_row(&mut db.table, 1)?;
        assert_eq!(expected_table_row_internal, table_row_internal);
    }
    // row in secondary index
    {
        let secondary_index = &mut db.secondary_indexes[0];
        let index_row = schema::table_row_to_index_row(
            &insert_operation.row,
            &secondary_index.metadata.schema.as_ref().unwrap(),
            1,
        );
        let expected_index_row_internal =
            schema::row_to_internal_row(&index_row, &secondary_index.metadata.schema);
        let index_row_internal = table::read_row(secondary_index, 2)?;
        assert_eq!(expected_index_row_internal, index_row_internal);
    }

    Ok(())
}

#[test]
fn read_single_success() -> Result<(), Error> {
    let mut context = setup();
    let mut db = create_for_test(context.config, context.schema)?;

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
