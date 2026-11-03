//! Cluster admission control for a SQL gateway.
//!
//! A gateway fronts several *groups* of engine clusters (Trino, StarRocks, ...).
//! Every incoming query must be admitted to one cluster of its target group
//! before it is forwarded, and each cluster accepts at most `max_running`
//! concurrent queries. The gateway handles thousands of queries per second,
//! so `try_acquire` sits on the hot path of every request.
//!
//! See README.md for the tasks.

use std::collections::HashMap;
use std::future::Future;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub name: String,
    pub max_running: u64,
}

impl ClusterConfig {
    pub fn new(name: impl Into<String>, max_running: u64) -> Self {
        Self {
            name: name.into(),
            max_running,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolConfig {
    pub groups: HashMap<String, Vec<ClusterConfig>>,
}

impl PoolConfig {
    pub fn group(mut self, name: impl Into<String>, clusters: Vec<ClusterConfig>) -> Self {
        self.groups.insert(name.into(), clusters);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// The group does not exist in the current config.
    UnknownGroup,
    /// Every cluster in the group is at capacity.
    Busy,
    /// `acquire_timeout` waited for its full duration without getting a slot.
    Timeout,
}

#[derive(Debug)]
struct ClusterState {
    name: String,
    max_running: AtomicU64,
    running: AtomicU64,
    admissions: Arc<AtomicU64>,
}

impl ClusterState {
    fn new(config: &ClusterConfig, admissions: Arc<AtomicU64>) -> Self {
        Self {
            name: config.name.clone(),
            max_running: AtomicU64::new(config.max_running),
            running: AtomicU64::new(0),
            admissions,
        }
    }

    fn running(&self) -> u64 {
        self.running.load(Ordering::Relaxed)
    }

    fn max_running(&self) -> u64 {
        self.max_running.load(Ordering::Relaxed)
    }

    fn available(&self) -> u64 {
        //self.max_running() - self.running() ---> unsigned integer cant see the negatives: 10 - 30 = ?

        self.max_running().saturating_sub(self.running())
    }

    fn try_increment(&self) -> bool {
        let mut current = self.running.load(Ordering::Relaxed);

        loop {
            if current >= self.max_running() {
                return false;
            }

            match self.running.compare_exchange(
                current,
                current + 1, // Change running from current to current + 1 if the current value is still equal to current
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn decrement(&self) {
        self.running.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct ClusterPool {
    groups: RwLock<HashMap<String, Vec<Arc<ClusterState>>>>,
    admissions: Mutex<HashMap<(String, String), Arc<AtomicU64>>>,
    next_start: AtomicUsize,
}

impl ClusterPool {
    pub fn new(config: PoolConfig) -> Arc<Self> {
        let mut admissions = HashMap::new();
        let groups = build_groups(&config, &mut admissions);

        Arc::new(Self {
            groups: RwLock::new(groups),
            admissions: Mutex::new(admissions),
            next_start: AtomicUsize::new(0),
        })
    }

    fn clusters(&self, group: &str) -> Option<Vec<Arc<ClusterState>>> {
        self.groups.read().get(&group.to_string()).cloned()
    }

    fn find(&self, group: &str, cluster: &str) -> Option<Arc<ClusterState>> {
        self.clusters(group)?
            .into_iter()
            .find(|c| c.name == cluster)
    }

    /// Admit one query to the least-loaded cluster of `group`, or fail fast with `Busy`.

    pub fn try_acquire(self: &Arc<Self>, group: &str) -> Result<Permit, AcquireError> {
        let state = {
            let groups = self.groups.read();
            let clusters = groups.get(group).ok_or(AcquireError::UnknownGroup)?;

            if clusters.is_empty() {
                return Err(AcquireError::Busy);
            }

            let start = self.next_start.fetch_add(1, Ordering::Relaxed) % clusters.len();

            let best = (0..clusters.len())
                .map(|offset| &clusters[(start + offset) % clusters.len()])
                .max_by_key(|state| state.available())
                .ok_or(AcquireError::Busy)?;

            let selected = if best.try_increment() {
                Some(best)
            } else {
                clusters
                    .iter()
                    .filter(|candidate| !Arc::ptr_eq(candidate, best))
                    .find(|candidate| candidate.try_increment())
            };

            Arc::clone(selected.ok_or(AcquireError::Busy)?)
        };

        state.admissions.fetch_add(1, Ordering::Relaxed);

        Ok(Permit {
            pool: Arc::clone(self),
            group: group.to_string(),
            cluster: state.name.clone(),
            state,
        })
    }

    /// Part 3: like `try_acquire`, but wait up to `wait` for a slot to free up.
    pub async fn acquire_timeout(
        self: &Arc<Self>,
        group: &str,
        wait: Duration,
    ) -> Result<Permit, AcquireError> {
        let _ = (group, wait);
        todo!("Part 3")
    }

    fn release(&self, _group: &str, state: &ClusterState) {
        state.decrement();
    }

    /// Current running count for a cluster, or `None` if it is not in the current config.
    pub fn running(&self, group: &str, cluster: &str) -> Option<u64> {
        self.find(group, cluster).map(|c| c.running())
    }

    /// Number of callers currently waiting inside `acquire_timeout` for `group`.
    pub fn queued(&self, group: &str) -> u64 {
        let _ = group;
        todo!("Part 3")
    }

    /// Total queries ever admitted to a cluster (a metric scraped by Prometheus).
    pub fn admissions(&self, group: &str, cluster: &str) -> u64 {
        self.admissions
            .lock()
            .unwrap()
            .get(&(group.to_owned(), cluster.to_owned()))
            .map(|counter| counter.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Admin API: change a cluster's capacity at runtime.
    pub fn set_max_running(&self, group: &str, cluster: &str, max_running: u64) {
        if let Some(state) = self.find(group, cluster) {
            state.max_running.store(max_running, Ordering::Relaxed);
        }
    }

    /// Bonus: swap in a new config while queries are in flight.
    ///
    /// A cluster keeps its identity across reloads by (group, name). Clusters removed
    /// from the config take no new queries; clusters added to it take traffic at once.
    pub fn reload(&self, config: PoolConfig) {
        let mut admissions = self.admissions.lock().unwrap();
        let groups = build_groups(&config, &mut admissions);

        *self.groups.write() = groups;
    }

    /// Bonus: correct a cluster's running counter against the engine's own count.
    /// `fetch` asks the engine how many queries it is running; it is slow.
    pub async fn reconcile<F, Fut>(&self, group: &str, cluster: &str, fetch: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = u64>,
    {
        let target = fetch().await;
        if let Some(state) = self.find(group, cluster) {
            state.running.store(target, Ordering::Relaxed);
        }
    }
}

fn build_groups(
    config: &PoolConfig,
    admissions: &mut HashMap<(String, String), Arc<AtomicU64>>,
) -> HashMap<String, Vec<Arc<ClusterState>>> {
    config
        .groups
        .iter()
        .map(|(group, clusters)| {
            let states = clusters
                .iter()
                .map(|cluster| {
                    let key = (group.clone(), cluster.name.clone());

                    let counter = Arc::clone(
                        admissions
                            .entry(key)
                            .or_insert_with(|| Arc::new(AtomicU64::new(0))),
                    );

                    Arc::new(ClusterState::new(cluster, counter))
                })
                .collect();

            (group.clone(), states)
        })
        .collect()
}

/// Proof of admission. The holder may run one query on `cluster()`.
pub struct Permit {
    pool: Arc<ClusterPool>,
    group: String,
    cluster: String,
    state: Arc<ClusterState>,
}

impl Permit {
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    /// Give the slot back once the query has finished.
    pub fn release(self) {
        //self.pool.release(&self.group, &self.cluster);  I will move this
        drop(self);
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.pool.release(&self.group, &self.state);
    }
}
