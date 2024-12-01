use std::sync::Mutex;

static global_stats: Mutex<Stats> = Mutex::new(
    Stats {
        num_chunk_reads: 0,
        num_chunk_writes: 0,
    });

#[derive(Debug)]
struct Stats {
    num_chunk_reads: u64,
    num_chunk_writes: u64,
}

pub fn increment_chunk_write() {
    let mut stats = global_stats.lock().unwrap();
    stats.num_chunk_writes += 1;
}

pub fn increment_chunk_read() {
    let mut stats = global_stats.lock().unwrap();
    stats.num_chunk_reads += 1;
}

pub fn print_stats() {
    let mut stats = global_stats.lock().unwrap();
    log::info!("{:#?}", stats);
}
