use std::sync::{Arc, Barrier};
use std::thread;

use admission::{ClusterConfig, ClusterPool, Permit, PoolConfig};

#[test]
fn inc_1_never_admits_more_than_capacity() {
    const THREADS: usize = 32;

    for _ in 0..100 {
        let pool = ClusterPool::new(
            PoolConfig::default().group("g", vec![ClusterConfig::new("trino-1", 1)]),
        );

        let barrier = Arc::new(Barrier::new(THREADS + 1));
        let mut handles = Vec::new();

        for _ in 0..THREADS {
            let pool = Arc::clone(&pool);
            let barrier = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                barrier.wait();
                pool.try_acquire("g").ok()
            }));
        }

        barrier.wait();

        let permits: Vec<Permit> = handles
            .into_iter()
            .filter_map(|handle| handle.join().unwrap())
            .collect();

        assert!(permits.len() <= 1, "More than one query was admitted");

        assert_eq!(pool.running("g", "trino-1"), Some(permits.len() as u64));

        for permit in permits {
            permit.release();
        }

        assert_eq!(pool.running("g", "trino-1"), Some(0));
    }
}

#[test]
fn inc_2_capacity_cut_does_not_block_other_clusters() {
    let pool = ClusterPool::new(PoolConfig::default().group(
        "g",
        vec![
            ClusterConfig::new("trino-busy", 40),
            ClusterConfig::new("trino-idle", 1),
        ],
    ));

    // Reserve 30 slots on the first cluster.
    let permits: Vec<_> = (0..30).map(|_| pool.try_acquire("g").unwrap()).collect();

    assert_eq!(pool.running("g", "trino-busy"), Some(30));
    assert_eq!(pool.running("g", "trino-idle"), Some(0));

    // Lower capacity below the number of running queries.
    pool.set_max_running("g", "trino-busy", 10);

    // The second cluster still has one free slot.
    let permit = pool
        .try_acquire("g")
        .expect("Idle cluster should accept a query");

    assert_eq!(permit.cluster(), "trino-idle");

    permit.release();

    for permit in permits {
        permit.release();
    }
}
