use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kafka_lite::store::broker::Broker;
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::tempdir;

// Single producer append throughput

fn bench_broker_single_producer(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_append");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_producer", |b| {
        let dir = tempdir().unwrap();
        let mut broker = Broker::try_new(dir.path()).unwrap();
        broker.create_topic("orders").unwrap();

        b.iter(|| {
            broker.append("orders", b"hello world").unwrap();
        });
    });

    group.finish();
}

// Single consumer read throughput

fn bench_broker_single_consumer(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_read");

    for msg_count in [1_000usize, 10_000] {
        group.throughput(Throughput::Elements(msg_count as u64));
        group.bench_with_input(
            BenchmarkId::new("messages_in_log", msg_count),
            &msg_count,
            |b, &msg_count| {
                let dir = tempdir().unwrap();
                let mut broker = Broker::try_new(dir.path()).unwrap();
                broker.create_topic("orders").unwrap();
                for _ in 0..msg_count {
                    broker.append("orders", b"payload").unwrap();
                }
                // seek consumer to start so it reads everything
                broker.seek("orders", "svc-a", 0).unwrap();

                b.iter(|| {
                    let result = broker.read_from("orders", "svc-a").unwrap();
                    assert!(!result.messages.is_empty());
                });
            },
        );
    }

    group.finish();
}

// Concurrent producer throughput

fn bench_broker_concurrent_producers(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_concurrent_append");

    for n_threads in [1usize, 2, 4, 8] {
        group.throughput(Throughput::Elements(n_threads as u64 * 100));
        group.bench_with_input(
            BenchmarkId::new("producers", n_threads),
            &n_threads,
            |b, &n_threads| {
                let dir = tempdir().unwrap();
                let mut broker = Broker::try_new(dir.path()).unwrap();
                broker.create_topic("orders").unwrap();
                let broker = Arc::new(Mutex::new(broker));

                b.iter(|| {
                    let handles: Vec<_> = (0..n_threads)
                        .map(|_| {
                            let broker = Arc::clone(&broker);
                            thread::spawn(move || {
                                for _ in 0..100 {
                                    broker.lock().unwrap().append("orders", b"msg").unwrap();
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

// Mixed producer + consumer throughput

fn bench_broker_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_mixed_workload");

    group.bench_function("4_producers_4_consumers", |b| {
        let dir = tempdir().unwrap();
        let mut broker = Broker::try_new(dir.path()).unwrap();
        broker.create_topic("orders").unwrap();
        // seed some messages so consumers have something to read
        for _ in 0..1000 {
            broker.append("orders", b"seed").unwrap();
        }
        // seek all consumers to start
        for i in 0..4 {
            broker.seek("orders", &format!("consumer-{}", i), 0).unwrap();
        }
        let broker = Arc::new(Mutex::new(broker));

        b.iter(|| {
            let mut handles = vec![];

            // 4 producers
            for _ in 0..4 {
                let broker = Arc::clone(&broker);
                handles.push(thread::spawn(move || {
                    for _ in 0..50 {
                        broker.lock().unwrap().append("orders", b"msg").unwrap();
                    }
                }));
            }

            // 4 consumers
            for i in 0..4 {
                let broker = Arc::clone(&broker);
                handles.push(thread::spawn(move || {
                    let consumer_id = format!("consumer-{}", i);
                    for _ in 0..50 {
                        broker.lock().unwrap().read_from("orders", &consumer_id).unwrap();
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(
    broker_benches,
    bench_broker_single_producer,
    bench_broker_single_consumer,
    bench_broker_concurrent_producers,
    bench_broker_mixed_workload,
);
criterion_main!(broker_benches);
