use crate::cache::Cache;
use crate::chunk;
use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::schema;
use crate::table::Table;
use crate::CHUNK_SIZE;
use protobuf::text_format::parse_from_str;
use std::io::Cursor;

struct TestContext {
    file: Cursor<Vec<u8>>,
    schema: TableSchema,
    cache: Cache,
}

fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    TestContext {
        file: std::io::Cursor::<Vec<u8>>::new(Vec::new()),
        schema: parse_from_str::<TableSchema>(
            "
            key {
                name: \"Key\"
                column_type: INTEGER
            }
            ",
        )
        .unwrap(),
        cache: Cache::default(),
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

#[tokio::test]
async fn create_ok() -> Result<(), Error> {
    let context = setup();
    let mut table = Table::create(context.file, "TestTable".to_string(), 0, context.schema).await?;

    assert_eq!(table.file.get_ref().len(), (CHUNK_SIZE * 2) as usize);

    let metadata: TableMetadataProto = chunk::read_chunk_at(&mut table.file, 0).await?;
    assert_eq!(metadata.root_chunk_offset, 1);
    assert_eq!(metadata.next_chunk_offset, 2);

    let node: NodeProto = chunk::read_chunk_at(&mut table.file, 1).await?;
    assert_eq!(node.offset, 1);

    Ok(())
}

#[tokio::test]
async fn insert_single_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = Table::create(context.file, "TestTable".to_string(), 0, context.schema).await?;

    let mut col = ValueProto::new();
    col.set_int_value(1);
    let mut row = InternalRowProto::new();
    row.col_values.push(col);
    table.insert(&mut context.cache, 1, row.clone()).await?;

    assert_eq!(table.file.get_ref().len(), (CHUNK_SIZE * 3) as usize);

    let root: NodeProto = chunk::read_chunk_at(&mut table.file, 1).await?;
    assert_eq!(root.offset, 1);
    assert!(root.has_internal());
    assert_eq!(root.internal().keys.len(), 0);
    assert_eq!(root.internal().child_offsets.len(), 1);
    assert_eq!(root.internal().child_offsets[0], 2);

    let data: NodeProto = chunk::read_chunk_at(&mut table.file, 2).await?;
    assert!(data.has_leaf());
    assert_eq!(data.offset, 2);
    assert_eq!(row, data.leaf().rows[0]);

    Ok(())
}

#[tokio::test]
async fn insert_sorted() -> Result<(), Error> {
    let mut context = setup();
    let mut table = Table::create(context.file, "TestTable".to_string(), 0, context.schema).await?;

    let mut col_1 = ValueProto::new();
    col_1.set_int_value(1);
    let mut row_1 = InternalRowProto::new();
    row_1.col_values.push(col_1);
    table.insert(&mut context.cache, 1, row_1.clone()).await?;

    let mut col_2 = ValueProto::new();
    col_2.set_int_value(2);
    let mut row_2 = InternalRowProto::new();
    row_2.col_values.push(col_2);
    table.insert(&mut context.cache, 2, row_2.clone()).await?;

    let mut col_3 = ValueProto::new();
    col_3.set_int_value(3);
    let mut row_3 = InternalRowProto::new();
    row_3.col_values.push(col_3);
    table.insert(&mut context.cache, 3, row_3.clone()).await?;

    assert_eq!(table.file.get_ref().len(), (CHUNK_SIZE * 3) as usize);
    let root: NodeProto = chunk::read_chunk_at(&mut table.file, 1).await?;
    assert_eq!(root.offset, 1);
    assert!(root.has_internal());
    assert_eq!(root.internal().child_offsets.len(), 1);

    let data: NodeProto = chunk::read_chunk_at(&mut table.file, 2).await?;
    assert_eq!(data.offset, 2);
    assert!(data.has_leaf());
    assert_eq!(data.leaf().rows.len(), 3);

    assert_eq!(row_1, data.leaf().rows[0]);
    assert_eq!(row_2, data.leaf().rows[1]);
    assert_eq!(row_3, data.leaf().rows[2]);

    Ok(())
}

#[tokio::test]
async fn insert_many_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = Table::create(context.file, "TestTable".to_string(), 0, context.schema).await?;

    for i in 0..60 {
        let mut col = ValueProto::new();
        col.set_int_value(i);
        let mut row = InternalRowProto::new();
        row.col_values.push(col);

        table.insert(&mut context.cache, i as u32, row).await?;
    }

    let metadata: TableMetadataProto = chunk::read_chunk_at(&mut table.file, 0).await?;
    assert_eq!(metadata.next_chunk_offset, 3);
    assert_eq!(metadata.root_chunk_offset, 1);

    assert_eq!(table.file.get_ref().len(), (CHUNK_SIZE * 3) as usize);
    for i in 1..3 {
        let node: NodeProto = chunk::read_chunk_at(&mut table.file, i).await?;
        validate_node_sorted(&node);
    }

    Ok(())
}

#[tokio::test]
async fn read_row_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = Table::create(
        context.file,
        "TestTable".to_string(),
        0,
        context.schema.clone(),
    )
    .await?;
    let row = parse_from_str::<InternalRowProto>("col_values { int_value: 1 }")?;
    table.insert(&mut context.cache, 1, row.clone()).await?;
    let read_result: RowProto = table.read_row(&mut context.cache, 1).await?;
    assert_eq!(
        read_result,
        schema::internal_row_to_row(&row, &context.schema)
    );
    Ok(())
}

#[tokio::test]
async fn read_row_many_ok() -> Result<(), Error> {
    let mut context = setup();
    let mut table = Table::create(context.file, "TestTable".to_string(), 0, context.schema).await?;
    let num_iter = 100;

    for i in 0..num_iter {
        let mut col = ValueProto::new();
        col.set_int_value(i);
        let mut row = InternalRowProto::new();
        row.col_values.push(col);

        table.insert(&mut context.cache, i as u32, row).await?;
    }

    for i in 0..num_iter {
        let read_result = table.read_row(&mut context.cache, i as u32).await?;

        let mut expected_col_val = ColumnProto::new();
        expected_col_val.name = "Key".to_string();
        expected_col_val
            .value
            .mut_or_insert_default()
            .set_int_value(i);
        let mut expected_read_result = RowProto::new();
        expected_read_result.columns.push(expected_col_val);

        assert_eq!(read_result, expected_read_result);
    }

    Ok(())
}
