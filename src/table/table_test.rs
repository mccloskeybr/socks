use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::table::file::*;
use crate::table::parse::*;
use crate::table::table::*;
use protobuf::text_format::parse_from_str;
use std::io::Cursor;

struct TestContext {
    file: std::io::Cursor<Vec<u8>>,
    table_config: TableConfig,
    db_config: DatabaseConfig,
}

fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    TestContext {
        file: std::io::Cursor::<Vec<u8>>::new(Vec::new()),
        table_config: parse_from_str::<TableConfig>(
            "
            insert_method: AGGRESSIVE_SPLIT
            read_method: BINARY_SEARCH
            schema {
                name: \"TestTable\"
                columns {
                    name: \"Key\"
                    type: INTEGER
                    is_key: true
                }
            }",
        )
        .unwrap(),
        db_config: parse_from_str::<DatabaseConfig>(
            "
            file {
                chunk_size: 512
                chunk_overflow_size: 10
            }",
        )
        .unwrap(),
    }
}

fn validate_node_sorted(node: &NodeProto) {
    match &node.node_type {
        Some(node_proto::Node_type::Internal(internal)) => {
            assert!(internal.keys.is_sorted());
        }
        Some(node_proto::Node_type::Leaf(leaf)) => {
            assert!(leaf.keys.is_sorted());
        }
        None => {}
    }
}

#[test]
fn create_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = Table::create(&mut context.file, context.db_config, context.table_config)?;
    assert_eq!(
        table.file.get_ref().len(),
        (table.db_config.file.chunk_size * 2) as usize
    );
    let chunk_0 = chunk::read_chunk_at(&table.db_config.file, &mut table.file, 0)?;
    let chunk_1 = chunk::read_chunk_at(&table.db_config.file, &mut table.file, 1)?;

    assert!(chunk_0.has_metadata());
    let metadata = chunk_0.metadata();
    assert_eq!(metadata.root_chunk_offset, 1);
    assert_eq!(metadata.next_chunk_offset, 2);

    assert!(chunk_1.has_node());
    let node = chunk_1.node();
    assert_eq!(node.offset, 1);

    Ok(())
}

#[test]
fn insert_single_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = Table::create(
        &mut context.file,
        context.db_config,
        context.table_config.clone(),
    )?;

    let op = parse_from_str::<InsertProto>(
        "
            table_name: \"TestTable\"
            column_values {
                name: \"Key\"
                int_value: 1
            }",
    )?;
    table.insert(op.clone())?;

    assert_eq!(
        table.file.get_ref().len(),
        (table.db_config.file.chunk_size * 3) as usize
    );

    let root_chunk = chunk::read_chunk_at(&table.db_config.file, &mut table.file, 1)?;
    assert!(root_chunk.has_node());
    let root = root_chunk.node();
    assert_eq!(root.offset, 1);
    assert!(root.has_internal());
    assert_eq!(root.internal().keys.len(), 0);
    assert_eq!(root.internal().child_offsets.len(), 1);
    assert_eq!(root.internal().child_offsets[0], 2);

    let data_chunk = chunk::read_chunk_at(&table.db_config.file, &mut table.file, 2)?;
    assert!(data_chunk.has_node());
    let data = data_chunk.node();
    assert!(data.has_leaf());
    assert_eq!(data.offset, 2);
    assert_eq!(
        (1, data.leaf().rows[0].clone()),
        transform::insert_op(op, &table.metadata.config.schema)
    );

    Ok(())
}

#[test]
fn insert_sorted() -> Result<(), Error> {
    let mut context = setup();
    let mut table = Table::create(
        &mut context.file,
        context.db_config,
        context.table_config.clone(),
    )?;

    let op_1 = parse_from_str::<InsertProto>(
        "
            table_name: \"TestTable\"
            column_values {
                name: \"Key\"
                int_value: 1
            }",
    )?;
    let op_2 = parse_from_str::<InsertProto>(
        "
            table_name: \"TestTable\"
            column_values {
                name: \"Key\"
                int_value: 2
            }",
    )?;
    let op_3 = parse_from_str::<InsertProto>(
        "
            table_name: \"TestTable\"
            column_values {
                name: \"Key\"
                int_value: 3
            }",
    )?;
    table.insert(op_1.clone())?;
    table.insert(op_2.clone())?;
    table.insert(op_3.clone())?;

    assert_eq!(
        table.file.get_ref().len(),
        (table.db_config.file.chunk_size * 3) as usize
    );
    let root_chunk = chunk::read_chunk_at(&table.db_config.file, &mut table.file, 1)?;
    assert!(root_chunk.has_node());
    let root = root_chunk.node();
    assert_eq!(root.offset, 1);
    assert!(root.has_internal());
    assert_eq!(root.internal().child_offsets.len(), 1);

    let data_chunk = chunk::read_chunk_at(&table.db_config.file, &mut table.file, 2)?;
    assert!(data_chunk.has_node());
    let data = data_chunk.node();
    assert_eq!(data.offset, 2);
    assert!(data.has_leaf());
    assert_eq!(data.leaf().rows.len(), 3);

    let row_1 = &data.leaf().rows[0];
    let row_2 = &data.leaf().rows[1];
    let row_3 = &data.leaf().rows[2];
    assert_eq!(
        (1, row_1.clone()),
        transform::insert_op(op_1, &table.metadata.config.schema)
    );
    assert_eq!(
        (2, row_2.clone()),
        transform::insert_op(op_2, &table.metadata.config.schema)
    );
    assert_eq!(
        (3, row_3.clone()),
        transform::insert_op(op_3, &table.metadata.config.schema)
    );

    Ok(())
}

#[test]
fn insert_many_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = Table::create(
        &mut context.file,
        context.db_config,
        context.table_config.clone(),
    )?;

    for i in 0..60 {
        let mut col_val = ColumnValueProto::new();
        col_val.name = "Key".into();
        col_val.set_int_value(i as i32);

        let mut op = InsertProto::new();
        op.table_name = "TestTable".into();
        op.column_values.push(col_val);

        table.insert(op)?;
    }

    let metadata = chunk::read_chunk_at(&table.db_config.file, &mut table.file, 0)?;
    let metadata = metadata.metadata();
    assert_eq!(metadata.next_chunk_offset, 3);
    assert_eq!(metadata.root_chunk_offset, 1);

    assert_eq!(
        table.file.get_ref().len(),
        (table.db_config.file.chunk_size * 3) as usize
    );
    for i in 1..3 {
        let node_chunk = chunk::read_chunk_at(&table.db_config.file, &mut table.file, i)?;
        validate_node_sorted(node_chunk.node());
    }

    Ok(())
}

#[test]
fn read_row_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = Table::create(
        &mut context.file,
        context.db_config,
        context.table_config.clone(),
    )?;

    let insert_op = parse_from_str::<InsertProto>(
        "
            table_name: \"TestTable\"
            column_values {
                name: \"Key\"
                int_value: 1
            }",
    )?;
    table.insert(insert_op)?;

    let read_op = parse_from_str::<ReadRowProto>(
        "
            table_name: \"TestTable\"
            key_value {
                name: \"Key\"
                int_value: 1
            }",
    )?;
    let read_result: InternalRowProto = table.read_row(read_op)?;

    assert_eq!(
        read_result,
        parse_from_str::<InternalRowProto>(
            "
            col_values { int_value: 1 }"
        )?
    );
    Ok(())
}

#[test]
fn read_row_many_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = Table::create(
        &mut context.file,
        context.db_config,
        context.table_config.clone(),
    )?;
    let num_iter = 100;

    for i in 0..num_iter {
        let mut col_val = ColumnValueProto::new();
        col_val.name = "Key".into();
        col_val.set_int_value(i as i32);

        let mut op = InsertProto::new();
        op.table_name = "TestTable".into();
        op.column_values.push(col_val);

        table.insert(op)?;
    }
    for i in 0..num_iter {
        let mut key_val = ColumnValueProto::new();
        key_val.name = "Key".into();
        key_val.set_int_value(i as i32);

        let mut op = ReadRowProto::new();
        op.table_name = "TestTable".into();
        op.key_value = Some(key_val).into();

        let read_result = table.read_row(op)?;

        let mut expected_col_val = InternalColumnProto::new();
        expected_col_val.set_int_value(i);
        let mut expected_read_result = InternalRowProto::new();
        expected_read_result.col_values.push(expected_col_val);

        assert_eq!(read_result, expected_read_result);
    }

    Ok(())
}
