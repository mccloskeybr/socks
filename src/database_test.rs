use crate::buffer::Buffer;
use crate::database::Database;
use crate::error::{Error, ErrorKind::*};
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::schema;
use protobuf::text_format::parse_from_str;
use protobuf::MessageField;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::Mutex;

type QueryResultsBuffer = Buffer<Cursor<Vec<u8>>, InternalQueryResultsProto>;

struct TestContext {
    db: Arc<Database<Cursor<Vec<u8>>>>,
}

async fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    let schema = parse_from_str::<DatabaseSchema>(
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
    .unwrap();
    TestContext {
        db: Arc::new(Database::create("", schema).await.unwrap()),
    }
}

#[tokio::test]
async fn insert_single_success() -> Result<(), Error> {
    let ctx = setup().await;
    let db = ctx.db;

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
    )
    .unwrap();
    db.insert(insert_operation.clone()).await?;

    // row in primary index
    {
        let expected_table_row_internal = insert_operation.row.clone().unwrap();
        let table_row_internal = db.table.read_row(1).await?;
        assert_eq!(expected_table_row_internal, table_row_internal);
    }
    // row in secondary index
    {
        let secondary_index = &db.secondary_indexes[0];
        let index_row = schema::table_row_to_index_row(
            &insert_operation.row,
            &secondary_index.schema,
            &db.table.schema,
        );
        let expected_index_row_internal = index_row;
        let index_row_internal = secondary_index.read_row(2).await?;
        assert_eq!(expected_index_row_internal, index_row_internal);
    }

    Ok(())
}

#[tokio::test]
async fn read_single_success() -> Result<(), Error> {
    let ctx = setup().await;
    let db = ctx.db;

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
    )
    .unwrap();
    db.insert(insert_operation.clone()).await?;

    let read_operation = parse_from_str::<ReadRowProto>(
        "
        key {
            name: \"Key\"
            value {
                int_value: 1
            }
        }
        ",
    )
    .unwrap();
    let row = db.read_row(read_operation).await;

    assert_eq!(insert_operation.row.unwrap(), row.unwrap());

    Ok(())
}

#[tokio::test]
async fn query_success() -> Result<(), Error> {
    let ctx = setup().await;
    let db = ctx.db;

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
        db.insert(insert_operation).await?;
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
    )
    .unwrap();
    let query_results_file = db.query(query_operation).await?;
    let query_results =
        QueryResultsBuffer::read_from_file(Arc::new(Mutex::new(query_results_file)), 0).await?;

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
    )
    .unwrap();
    assert_eq!(query_results.data, expected_query_results);

    Ok(())
}

#[tokio::test]
async fn async_read_write_success() -> Result<(), Error> {
    let ctx = setup().await;
    let db = ctx.db;
    let num_iter = 100;

    let mut task_set = tokio::task::JoinSet::new();
    for i in 0..num_iter {
        let db = db.clone();
        task_set.spawn(async move {
            let insert_operation = parse_from_str::<InsertProto>(
                format!(
                    "
                row {{
                    columns {{
                        name: \"Key\"
                        value {{
                            int_value: {i}
                        }}
                    }}
                    columns {{
                        name: \"Value\"
                        value {{
                            int_value: {i}
                        }}
                    }}
                }}
                ",
                )
                .as_str(),
            )
            .unwrap();
            db.insert(insert_operation.clone()).await.unwrap();
        });
    }

    for i in 0..num_iter {
        let db = db.clone();
        tokio::spawn(async move {
            let read_operation = parse_from_str::<ReadRowProto>(
                format!(
                    "
                key {{
                    name: \"Key\"
                    value {{
                        int_value: {i}
                    }}
                }}
                ",
                )
                .as_str(),
            )
            .unwrap();
            let expected_row = parse_from_str::<RowProto>(
                format!(
                    "
                    columns {{
                        name: \"Key\"
                        value {{
                            int_value: {i}
                        }}
                    }}
                    columns {{
                        name: \"Value\"
                        value {{
                            int_value: {i}
                        }}
                    }}
                ",
                )
                .as_str(),
            )
            .unwrap();
            let row = db.read_row(read_operation).await.unwrap();
            assert_eq!(expected_row, row);
        });
    }

    Ok(())
}

#[tokio::test]
async fn delete_single_success() -> Result<(), Error> {
    let ctx = setup().await;
    let db = ctx.db;

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
    )
    .unwrap();
    db.insert(insert_operation.clone()).await?;

    let delete_operation = parse_from_str::<DeleteProto>(
        "
        key {
        name: \"Key\"
        value {
        int_value: 1
        }
        }
        ",
    )
    .unwrap();
    db.delete(delete_operation.clone()).await?;

    let table_row_internal = db.table.read_row(1).await;
    assert_eq!(table_row_internal.unwrap_err().kind, NotFound);

    let index_row_internal = db.secondary_indexes[0].read_row(2).await;
    assert_eq!(index_row_internal.unwrap_err().kind, NotFound);

    Ok(())
}
