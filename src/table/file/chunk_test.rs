use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::table::file::chunk::*;
use protobuf::text_format::parse_from_str;

struct TestContext {
    file: std::io::Cursor<Vec<u8>>,
    config: FileConfig,
}

fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    TestContext {
        file: std::io::Cursor::<Vec<u8>>::new(Vec::new()),
        config: parse_from_str::<FileConfig>(
            "
            chunk_size: 512
            chunk_overflow_size: 10",
        )
        .unwrap(),
    }
}

#[test]
fn read_write_single_chunk() -> Result<(), Error> {
    let mut context = setup();

    let mut chunk_in = ChunkProto::new();
    chunk_in.mut_metadata().next_chunk_offset = 1;
    write_chunk_at(&context.config, &mut context.file, &chunk_in, 0)?;
    assert_eq!(
        context.file.get_ref().len(),
        context.config.chunk_size as usize
    );

    let chunk_out = read_chunk_at(&context.config, &mut context.file, 0)?;
    assert_eq!(chunk_out, chunk_in);

    Ok(())
}

#[test]
fn read_write_many_chunks() -> Result<(), Error> {
    let mut context = setup();
    let n = 5;
    let mut chunks: Vec<ChunkProto> = Vec::new();
    for i in 0..n {
        let mut chunk = ChunkProto::new();
        chunk.mut_metadata().next_chunk_offset = i;
        chunks.push(chunk);
    }

    for i in 0..n {
        write_chunk_at(&context.config, &mut context.file, &chunks[i as usize], i)?;
    }
    assert_eq!(
        context.file.get_ref().len(),
        (context.config.chunk_size * n) as usize
    );

    for i in 0..n {
        let chunk = read_chunk_at(&context.config, &mut context.file, i)?;
        assert_eq!(chunk, chunks[i as usize]);
    }

    Ok(())
}

#[test]
fn overwrite_chunk() -> Result<(), Error> {
    let mut context = setup();

    let mut chunk_1 = ChunkProto::new();
    chunk_1.mut_metadata().next_chunk_offset = 1;
    write_chunk_at(&context.config, &mut context.file, &chunk_1, 0)?;
    assert_eq!(
        context.file.get_ref().len(),
        context.config.chunk_size as usize
    );

    let mut chunk_2 = ChunkProto::new();
    chunk_2.mut_metadata().next_chunk_offset = 2;
    write_chunk_at(&context.config, &mut context.file, &chunk_2, 0)?;
    assert_eq!(
        context.file.get_ref().len(),
        context.config.chunk_size as usize
    );

    let chunk = read_chunk_at(&context.config, &mut context.file, 0)?;
    assert_eq!(chunk, chunk_2);

    Ok(())
}

#[test]
fn huge_chunk_fails_write() -> Result<(), Error> {
    let mut context = setup();

    let mut chunk = ChunkProto::new();
    let node = chunk.mut_node();
    for i in 0..context.config.chunk_size as usize {
        node.mut_internal().keys.push(std::u32::MAX);
    }
    assert!(node.compute_size() > context.config.chunk_size as usize as u64);
    match write_chunk_at(&context.config, &mut context.file, &chunk, 0) {
        Err(Error::OutOfBounds(..)) => return Ok(()),
        _ => panic!(),
    }
}

#[test]
fn would_chunk_overflow_false() -> Result<(), Error> {
    let mut context = setup();
    assert!(!would_chunk_overflow(&context.config, 0));
    Ok(())
}

#[test]
fn would_chunk_overflow_true() -> Result<(), Error> {
    let mut context = setup();
    assert!(would_chunk_overflow(
        &context.config,
        context.config.chunk_size as usize
    ));
    Ok(())
}
