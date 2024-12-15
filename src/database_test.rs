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
        column_values {
            name: \"Key\"
            int_value: 1
        }
        column_values {
            name: \"Value\"
            int_value: 2
        }
        ",
    )?;
    insert(&mut db, insert_operation.clone())?;

    let expected_table_row =
        schema::to_row(&db.table.metadata.schema, &insert_operation.column_values);
    let table_row = table::read_row(&mut db.table, 1)?;
    assert_eq!(expected_table_row, table_row);

    let secondary_index = &mut db.secondary_indexes[0];
    let index_cols = schema::create_index_cols(
        &secondary_index.metadata.schema.as_ref().unwrap(),
        &insert_operation.column_values,
        1,
    );
    let expected_index_row = schema::to_row(&secondary_index.metadata.schema, &index_cols);
    let index_row = table::read_row(secondary_index, 2)?;
    assert_eq!(expected_table_row, table_row);

    Ok(())
}
