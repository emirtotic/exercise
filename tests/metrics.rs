use admission::{ClusterConfig, ClusterPool, PoolConfig};

//This test checks that booking success isnt affected by active queries and that history metrics survive a reload
#[test]
fn admissions_survive_release_and_reload() {
    let config = PoolConfig::default().group("g", vec![ClusterConfig::new("trino-1", 1)]);

    let pool = ClusterPool::new(config.clone());

    assert_eq!(pool.admissions("g", "trino-1"), 0);

    for expected in 1..=3 {
        let permit = pool.try_acquire("g").unwrap();

        assert_eq!(pool.admissions("g", "trino-1"), expected);

        permit.release();
    }

    pool.reload(config);

    assert_eq!(pool.admissions("g", "trino-1"), 3);

    let permit = pool.try_acquire("g").unwrap();
    permit.release();

    assert_eq!(pool.admissions("g", "trino-1"), 4);
}


#[test]
fn benchmark_workload_admits_every_request() {
    const THREADS: usize = 16;
    const REQUESTS: usize = 1_000;

    let clusters = (0..8)
        .map(|i| ClusterConfig::new(format!("trino-{i}"), 1_000_000))
        .collect();

    let pool = ClusterPool::new(
        PoolConfig::default().group("analytics", clusters),
    );

    let mut handles = Vec::new();

    for _ in 0..THREADS {
        let pool = std::sync::Arc::clone(&pool);

        handles.push(std::thread::spawn(move || {
            for _ in 0..REQUESTS {
                let permit = pool
                    .try_acquire("analytics")
                    .expect("Unexpected Busy in benchmark workload");

                permit.release();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total: u64 = (0..8)
        .map(|i| {
            pool.admissions("analytics", &format!("trino-{i}"))
        })
        .sum();

    assert_eq!(total, (THREADS * REQUESTS) as u64);
}

