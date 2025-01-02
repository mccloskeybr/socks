use crate::buffer::Buffer;
use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::BUFFER_SIZE;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::Mutex;

type MetadataBuffer = Buffer<Cursor<Vec<u8>>, TableMetadataProto>;

struct TestContext {
    file: Arc<Mutex<Cursor<Vec<u8>>>>,
}

fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    TestContext {
        file: Arc::new(Mutex::new(Cursor::<Vec<u8>>::new(Vec::new()))),
    }
}

#[tokio::test]
async fn read_write_single_buffer() -> Result<(), Error> {
    let context = setup();

    let mut metadata = TableMetadataProto::new();
    metadata.next_chunk_offset = 1;
    let mut buffer_out = MetadataBuffer::new_for_file(context.file.clone(), 0, metadata);
    buffer_out.write_to_file().await?;
    assert_eq!(context.file.lock().await.get_ref().len(), BUFFER_SIZE);

    let buffer_in = MetadataBuffer::read_from_file(context.file.clone(), 0).await?;
    assert_eq!(buffer_out.data, buffer_in.data);

    Ok(())
}

#[tokio::test]
async fn read_write_many_buffers() -> Result<(), Error> {
    let context = setup();
    let n = 5;

    let mut buffers: Vec<MetadataBuffer> = Vec::new();
    for i in 0..n {
        let mut metadata = TableMetadataProto::new();
        metadata.next_chunk_offset = i;
        let mut buffer = MetadataBuffer::new_for_file(context.file.clone(), i, metadata);
        buffer.write_to_file().await?;
        buffers.push(buffer);
    }
    assert_eq!(
        context.file.lock().await.get_ref().len(),
        BUFFER_SIZE * (n as usize)
    );

    for i in 0..n {
        let buffer = MetadataBuffer::read_from_file(context.file.clone(), i).await?;
        assert_eq!(buffer.data, buffers[i as usize].data);
    }

    Ok(())
}

#[tokio::test]
async fn overwrite_buffer() -> Result<(), Error> {
    let context = setup();

    let mut metadata_1 = TableMetadataProto::new();
    metadata_1.next_chunk_offset = 1;
    let mut buffer_1 = MetadataBuffer::new_for_file(context.file.clone(), 0, metadata_1);
    buffer_1.write_to_file().await?;
    assert_eq!(context.file.lock().await.get_ref().len(), BUFFER_SIZE);

    let mut metadata_2 = TableMetadataProto::new();
    metadata_2.next_chunk_offset = 2;
    let mut buffer_2 = MetadataBuffer::new_for_file(context.file.clone(), 0, metadata_2);
    buffer_2.write_to_file().await?;
    assert_eq!(
        context.file.lock().await.get_ref().len(),
        BUFFER_SIZE as usize
    );

    let buffer_final = MetadataBuffer::read_from_file(context.file.clone(), 0).await?;
    assert_eq!(buffer_final.data, buffer_2.data);

    Ok(())
}

#[tokio::test]
async fn would_buffer_overflow_false() -> Result<(), Error> {
    let context = setup();

    let buffer = MetadataBuffer::new_for_file(context.file.clone(), 0, TableMetadataProto::new());
    assert!(!buffer.would_overflow(0));
    Ok(())
}

#[tokio::test]
async fn would_buffer_overflow_true() -> Result<(), Error> {
    let context = setup();

    let buffer = MetadataBuffer::new_for_file(context.file.clone(), 0, TableMetadataProto::new());
    assert!(buffer.would_overflow(BUFFER_SIZE));
    Ok(())
}
