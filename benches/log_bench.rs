use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kafka_lite::store::log::Log;
use tempfile::{NamedTempFile, tempdir};

// Append throughput

fn bench_append_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_append");

    for size in [64usize, 1024, 65536] {
        let payload = vec![0u8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("payload_bytes", size), &payload, |b, payload| {
            let file = NamedTempFile::new().unwrap();
            let mut log = Log::try_new(file.path()).unwrap();
            b.iter(|| {
                log.append(payload).unwrap();
            });
        });
    }

    group.finish();
}

// Read throughput at varying log sizes 

fn bench_read_from(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_read_from");

    for msg_count in [1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(msg_count as u64));
        group.bench_with_input(
            BenchmarkId::new("log_size", msg_count),
            &msg_count,
            |b, &msg_count| {
                let file = NamedTempFile::new().unwrap();
                let mut log = Log::try_new(file.path()).unwrap();
                let payload = vec![0u8; 64];
                for _ in 0..msg_count {
                    log.append(&payload).unwrap();
                }
                b.iter(|| {
                    let msgs = log.read_from(0);
                    assert_eq!(msgs.len(), msg_count);
                });
            },
        );
    }

    group.finish();
}

// Replay time (startup cost) 

fn bench_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_replay");

    for msg_count in [1_000usize, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("messages", msg_count),
            &msg_count,
            |b, &msg_count| {
                // pre-populate the file outside the benchmark loop
                let file = NamedTempFile::new().unwrap();
                {
                    let mut log = Log::try_new(file.path()).unwrap();
                    let payload = vec![0u8; 64];
                    for _ in 0..msg_count {
                        log.append(&payload).unwrap();
                    }
                }
                // benchmark only the replay (Log::try_new reads and replays the file)
                b.iter(|| {
                    let _log = Log::try_new(file.path()).unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(log_benches, bench_append_small, bench_read_from, bench_replay);
criterion_main!(log_benches);
