//! Manual single-log benchmark. Release mode, warm buffered reads, real syncs.
//! Not a network, cold-cache, Linux durability or production latency benchmark.
use serapendiya::{domain::Offset, storage::Log};
use std::{
    fs,
    hint::black_box,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

fn report(name: &str, size: usize, group: usize, samples: &mut [u128]) {
    samples.sort_unstable();
    let percentile = |p: usize| samples[(samples.len() * p).div_ceil(1000) - 1];
    println!(
        "METRIC {name} value_bytes={size} group={group} n={} p50_ns={} p99_ns={} p999_ns={}",
        samples.len(),
        percentile(500),
        percentile(990),
        percentile(999)
    );
}

#[test]
#[ignore = "manual release benchmark with real filesystem syncs"]
fn measure_storage_operations() {
    for size in [32, 1024, 65536] {
        for group in [1, 16] {
            let mut append = Vec::new();
            let mut fetch = Vec::new();
            let mut restart = Vec::new();
            let mut total_append_ns = 0;
            for sample in 0..5 {
                let nonce = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let dir = std::env::temp_dir().join(format!(
                    "serapendiya-bench-{}-{nonce}-{sample}",
                    std::process::id()
                ));
                let payload: Vec<_> = (0..size).map(|i| i as u8).collect();
                let mut log = Log::open(&dir).unwrap();
                for commit in 0..64 {
                    let owned: Vec<Vec<u8>> = (0..group)
                        .map(|i| {
                            let mut value = payload.clone();
                            value[..8]
                                .copy_from_slice(&((commit * group + i) as u64).to_be_bytes());
                            value
                        })
                        .collect();
                    let values: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
                    let start = Instant::now();
                    black_box(log.append_group(black_box(&values)).unwrap());
                    let elapsed = start.elapsed().as_nanos();
                    append.push(elapsed);
                    total_append_ns += elapsed;
                }
                // Deterministic scattered reads over a recently written log.
                for i in 0..256 {
                    let offset = Offset::new(((i * 17) % (64 * group)) as u64);
                    let start = Instant::now();
                    let batch = log.read_batch(offset).unwrap().unwrap();
                    fetch.push(start.elapsed().as_nanos());
                    assert_eq!(batch.offset, offset);
                    assert_eq!(&batch.records[..8], &offset.get().to_be_bytes());
                    assert_eq!(&batch.records[8..], &payload[8..]);
                }
                drop(log);
                for _ in 0..3 {
                    let start = Instant::now();
                    let reopened = Log::open(&dir).unwrap();
                    restart.push(start.elapsed().as_nanos());
                    assert_eq!(reopened.next_offset().get(), (64 * group) as u64);
                    drop(reopened);
                }
                fs::remove_dir_all(dir).unwrap();
            }
            println!(
                "THROUGHPUT value_bytes={size} group={group} records_per_sec={:.1}",
                (320 * group) as f64 * 1e9 / total_append_ns as f64
            );
            report("append_group", size, group, &mut append);
            report("warm_fetch", size, group, &mut fetch);
            report("restart", size, group, &mut restart);
        }
    }
}
