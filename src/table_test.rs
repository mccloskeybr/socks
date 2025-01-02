use crate::buffer::Buffer;
use crate::buffer_pool::BufferPool;
use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::schema;
use crate::table::Table;
use crate::BUFFER_SIZE;
use protobuf::text_format::parse_from_str;
use std::io::Cursor;
use std::sync::Arc;

type MetadataBuffer = Buffer<Cursor<Vec<u8>>, TableMetadataProto>;
type NodeBuffer = Buffer<Cursor<Vec<u8>>, NodeProto>;

struct TestContext {
    file: Cursor<Vec<u8>>,
    schema: TableSchema,
    buffer_pool: BufferPool<Cursor<Vec<u8>>>,
}

fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    TestContext {
        file: Cursor::<Vec<u8>>::new(Vec::new()),
        schema: parse_from_str::<TableSchema>(
            "
            key {
                name: \"Key\"
                column_type: INTEGER
            }
            ",
        )
        .unwrap(),
        buffer_pool: BufferPool::new(),
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
    let table = Table::create(context.file, "TestTable".to_string(), 0, context.schema).await?;

    assert_eq!(
        table.file.lock().await.get_ref().len(),
        (BUFFER_SIZE * 2) as usize
    );

    let metadata = MetadataBuffer::read_from_file(table.file.clone(), 0).await?;
    assert_eq!(metadata.data.root_chunk_offset, 1);
    assert_eq!(metadata.data.next_chunk_offset, 2);

    let node = NodeBuffer::read_from_file(table.file.clone(), 1).await?;
    assert_eq!(node.data.offset, 1);

    Ok(())
}

#[tokio::test]
async fn insert_single_ok() -> Result<(), Error> {
    let mut context = setup();
    let table = Table::create(context.file, "TestTable".to_string(), 0, context.schema).await?;

    let mut col = ValueProto::new();
    col.set_int_value(1);
    let mut row = InternalRowProto::new();
    row.col_values.push(col);
    table
        .insert(&mut context.buffer_pool, 1, row.clone())
        .await?;

    context.buffer_pool.flush().await?;
    assert_eq!(
        table.file.lock().await.get_ref().len(),
        (BUFFER_SIZE * 3) as usize
    );

    let root = NodeBuffer::read_from_file(table.file.clone(), 1).await?;
    assert_eq!(root.data.offset, 1);
    assert!(root.data.has_internal());
    assert_eq!(root.data.internal().keys.len(), 0);
    assert_eq!(root.data.internal().child_offsets.len(), 1);
    assert_eq!(root.data.internal().child_offsets[0], 2);

    let child = NodeBuffer::read_from_file(table.file.clone(), 2).await?;
    assert!(child.data.has_leaf());
    assert_eq!(child.data.offset, 2);
    assert_eq!(row, child.data.leaf().rows[0]);

    Ok(())
}

#[tokio::test]
async fn insert_sorted() -> Result<(), Error> {
    let mut context = setup();
    let table = Table::create(context.file, "TestTable".to_string(), 0, context.schema).await?;

    let mut col_1 = ValueProto::new();
    col_1.set_int_value(1);
    let mut row_1 = InternalRowProto::new();
    row_1.col_values.push(col_1);
    table
        .insert(&mut context.buffer_pool, 1, row_1.clone())
        .await?;

    let mut col_2 = ValueProto::new();
    col_2.set_int_value(2);
    let mut row_2 = InternalRowProto::new();
    row_2.col_values.push(col_2);
    table
        .insert(&mut context.buffer_pool, 2, row_2.clone())
        .await?;

    let mut col_3 = ValueProto::new();
    col_3.set_int_value(3);
    let mut row_3 = InternalRowProto::new();
    row_3.col_values.push(col_3);
    table
        .insert(&mut context.buffer_pool, 3, row_3.clone())
        .await?;

    context.buffer_pool.flush().await?;
    assert_eq!(
        table.file.lock().await.get_ref().len(),
        (BUFFER_SIZE * 3) as usize
    );

    let root = NodeBuffer::read_from_file(table.file.clone(), 1).await?;
    assert_eq!(root.data.offset, 1);
    assert!(root.data.has_internal());
    assert_eq!(root.data.internal().child_offsets.len(), 1);

    let child = NodeBuffer::read_from_file(table.file.clone(), 2).await?;
    assert_eq!(child.data.offset, 2);
    assert!(child.data.has_leaf());
    assert_eq!(child.data.leaf().rows.len(), 3);

    assert_eq!(row_1, child.data.leaf().rows[0]);
    assert_eq!(row_2, child.data.leaf().rows[1]);
    assert_eq!(row_3, child.data.leaf().rows[2]);

    Ok(())
}

#[tokio::test]
async fn insert_with_split_ok() -> Result<(), Error> {
    let mut context = setup();
    let table = Table::create(context.file, "TestTable".to_string(), 0, context.schema).await?;

    for i in 0..500 {
        let mut col = ValueProto::new();
        col.set_int_value(i);
        let mut row = InternalRowProto::new();
        row.col_values.push(col);

        table
            .insert(&mut context.buffer_pool, i as u32, row)
            .await?;
    }

    context.buffer_pool.flush().await?;
    assert_eq!(
        table.file.lock().await.get_ref().len(),
        (BUFFER_SIZE * 4) as usize
    );

    let metadata = MetadataBuffer::read_from_file(table.file.clone(), 0).await?;
    assert_eq!(metadata.data.next_chunk_offset, 4);
    assert_eq!(metadata.data.root_chunk_offset, 1);

    for i in 1..2 {
        let node = NodeBuffer::read_from_file(table.file.clone(), i).await?;
        validate_node_sorted(&node.get());
    }

    Ok(())
}

#[tokio::test]
async fn read_row_ok() -> Result<(), Error> {
    let mut context = setup();
    let table = Table::create(
        context.file,
        "TestTable".to_string(),
        0,
        context.schema.clone(),
    )
    .await?;
    let row = parse_from_str::<InternalRowProto>("col_values { int_value: 1 }")?;
    table
        .insert(&mut context.buffer_pool, 1, row.clone())
        .await?;
    let read_result: RowProto = table.read_row(&mut context.buffer_pool, 1).await?;
    assert_eq!(
        read_result,
        schema::internal_row_to_row(&row, &context.schema)
    );
    Ok(())
}

#[tokio::test]
async fn read_row_many_ok() -> Result<(), Error> {
    let mut context = setup();
    let table = Table::create(context.file, "TestTable".to_string(), 0, context.schema).await?;
    let num_iter = 100;

    for i in 0..num_iter {
        let mut col = ValueProto::new();
        col.set_int_value(i);
        let mut row = InternalRowProto::new();
        row.col_values.push(col);

        table
            .insert(&mut context.buffer_pool, i as u32, row)
            .await?;
    }

    for i in 0..num_iter {
        let read_result = table.read_row(&mut context.buffer_pool, i as u32).await?;

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

#[tokio::test]
async fn async_read_write_success() -> Result<(), Error> {
    let mut context = setup();
    let table =
        Arc::new(Table::create(context.file, "TestTable".to_string(), 0, context.schema).await?);
    let num_iter = 100;

    let mut task_set = tokio::task::JoinSet::new();
    for i in 0..num_iter {
        let buffer_pool: &mut BufferPool<Cursor<Vec<u8>>> =
            unsafe { std::mem::transmute(&mut context.buffer_pool) };
        let table = table.clone();
        task_set.spawn(async move {
            let mut col = ValueProto::new();
            col.set_int_value(i);
            let mut row = InternalRowProto::new();
            row.col_values.push(col);

            table.insert(buffer_pool, i as u32, row).await.unwrap();
        });
    }
    task_set.join_all().await;

    for i in 0..num_iter {
        let buffer_pool: &mut BufferPool<Cursor<Vec<u8>>> =
            unsafe { std::mem::transmute(&mut context.buffer_pool) };
        let table = table.clone();
        tokio::spawn(async move {
            let read_result = table.read_row(buffer_pool, i as u32).await.unwrap();

            let mut expected_col_val = ColumnProto::new();
            expected_col_val.name = "Key".to_string();
            expected_col_val
                .value
                .mut_or_insert_default()
                .set_int_value(i);
            let mut expected_read_result = RowProto::new();
            expected_read_result.columns.push(expected_col_val);

            assert_eq!(read_result, expected_read_result);
        });
    }

    Ok(())
}
