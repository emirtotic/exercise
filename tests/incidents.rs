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
