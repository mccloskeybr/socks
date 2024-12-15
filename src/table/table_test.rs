use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::table::chunk;
use crate::table::table::*;
use protobuf::text_format::parse_from_str;
use std::io::Cursor;

struct TestContext {
    file: std::io::Cursor<Vec<u8>>,
    schema: TableSchema,
    config: TableConfig,
}

fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    TestContext {
        file: std::io::Cursor::<Vec<u8>>::new(Vec::new()),
        schema: parse_from_str::<TableSchema>(
            "
            columns {
                name: \"Key\"
                type: INTEGER
                is_key: true
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
    let mut table = create(&mut context.file, context.config, context.schema)?;
    assert_eq!(
        table.file.get_ref().len(),
        (table.metadata.config.chunk_size * 2) as usize
    );
    let chunk_0 = chunk::read_chunk_at(&table.metadata.config, &mut table.file, 0)?;
    let chunk_1 = chunk::read_chunk_at(&table.metadata.config, &mut table.file, 1)?;

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
    let mut table = create(&mut context.file, context.config, context.schema)?;

    let mut col = InternalColumnProto::new();
    col.set_int_value(1);
    let mut row = InternalRowProto::new();
    row.col_values.push(col);
    insert(&mut table, 1, row.clone())?;

    assert_eq!(
        table.file.get_ref().len(),
        (table.metadata.config.chunk_size * 3) as usize
    );

    let root_chunk = chunk::read_chunk_at(&table.metadata.config, &mut table.file, 1)?;
    assert!(root_chunk.has_node());
    let root = root_chunk.node();
    assert_eq!(root.offset, 1);
    assert!(root.has_internal());
    assert_eq!(root.internal().keys.len(), 0);
    assert_eq!(root.internal().child_offsets.len(), 1);
    assert_eq!(root.internal().child_offsets[0], 2);

    let data_chunk = chunk::read_chunk_at(&table.metadata.config, &mut table.file, 2)?;
    assert!(data_chunk.has_node());
    let data = data_chunk.node();
    assert!(data.has_leaf());
    assert_eq!(data.offset, 2);
    assert_eq!(row, data.leaf().rows[0]);

    Ok(())
}

#[test]
fn insert_sorted() -> Result<(), Error> {
    let mut context = setup();
    let mut table = create(&mut context.file, context.config, context.schema)?;

    let mut col_1 = InternalColumnProto::new();
    col_1.set_int_value(1);
    let mut row_1 = InternalRowProto::new();
    row_1.col_values.push(col_1);
    insert(&mut table, 1, row_1.clone())?;

    let mut col_2 = InternalColumnProto::new();
    col_2.set_int_value(2);
    let mut row_2 = InternalRowProto::new();
    row_2.col_values.push(col_2);
    insert(&mut table, 2, row_2.clone())?;

    let mut col_3 = InternalColumnProto::new();
    col_3.set_int_value(3);
    let mut row_3 = InternalRowProto::new();
    row_3.col_values.push(col_3);
    insert(&mut table, 3, row_3.clone())?;

    assert_eq!(
        table.file.get_ref().len(),
        (table.metadata.config.chunk_size * 3) as usize
    );
    let root_chunk = chunk::read_chunk_at(&table.metadata.config, &mut table.file, 1)?;
    assert!(root_chunk.has_node());
    let root = root_chunk.node();
    assert_eq!(root.offset, 1);
    assert!(root.has_internal());
    assert_eq!(root.internal().child_offsets.len(), 1);

    let data_chunk = chunk::read_chunk_at(&table.metadata.config, &mut table.file, 2)?;
    assert!(data_chunk.has_node());
    let data = data_chunk.node();
    assert_eq!(data.offset, 2);
    assert!(data.has_leaf());
    assert_eq!(data.leaf().rows.len(), 3);

    assert_eq!(row_1, data.leaf().rows[0]);
    assert_eq!(row_2, data.leaf().rows[1]);
    assert_eq!(row_3, data.leaf().rows[2]);

    Ok(())
}

#[test]
fn insert_many_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = create(&mut context.file, context.config, context.schema)?;

    for i in 0..60 {
        let mut col = InternalColumnProto::new();
        col.set_int_value(i);
        let mut row = InternalRowProto::new();
        row.col_values.push(col);

        insert(&mut table, i as u32, row)?;
    }

    let metadata = chunk::read_chunk_at(&table.metadata.config, &mut table.file, 0)?;
    let metadata = metadata.metadata();
    assert_eq!(metadata.next_chunk_offset, 3);
    assert_eq!(metadata.root_chunk_offset, 1);

    assert_eq!(
        table.file.get_ref().len(),
        (table.metadata.config.chunk_size * 3) as usize
    );
    for i in 1..3 {
        let node_chunk = chunk::read_chunk_at(&table.metadata.config, &mut table.file, i)?;
        validate_node_sorted(node_chunk.node());
    }

    Ok(())
}

#[test]
fn read_row_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = create(&mut context.file, context.config, context.schema)?;
    let row = parse_from_str::<InternalRowProto>("col_values { int_value: 1 }")?;
    insert(&mut table, 1, row.clone())?;
    let read_result: InternalRowProto = read_row(&mut table, 1)?;
    assert_eq!(read_result, row);
    Ok(())
}

#[test]
fn read_row_many_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = create(&mut context.file, context.config, context.schema)?;
    let num_iter = 100;

    for i in 0..num_iter {
        let mut col = InternalColumnProto::new();
        col.set_int_value(i);
        let mut row = InternalRowProto::new();
        row.col_values.push(col);

        insert(&mut table, i as u32, row)?;
    }

    for i in 0..num_iter {
        let read_result = read_row(&mut table, i as u32)?;

        let mut expected_col_val = InternalColumnProto::new();
        expected_col_val.set_int_value(i);
        let mut expected_read_result = InternalRowProto::new();
        expected_read_result.col_values.push(expected_col_val);

        assert_eq!(read_result, expected_read_result);
    }

    Ok(())
}
