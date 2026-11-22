//! The delivery half of the concurrent outbox dispatcher: per-instance chains under two bounds.
//!
//! Every claimed or handed-off row enters a [`DeliveryScheduler`]. Rows of one process instance
//! form one **chain**, delivered one after another in the order they were submitted (the claim
//! order, `next_attempt_at` then `created_at`). Rows of different instances never share a queue,
//! so a slow send holds up only the rest of its own instance's rows.
//!
//! Three bounds apply, taken in this order:
//!
//! 1. **Buffer** (`max_buffered`): the rows held in memory, in flight or queued. [`submit`] waits
//!    for room here and nowhere else, so one busy instance can never stall the claimer.
//! 2. **Destination** (`per_destination`): the sends in flight to one destination authority
//!    (scheme + host + port, [`crate::sink::destination_authority`]).
//! 3. **Global** (`workers`): the sends in flight across every destination. Taken after the
//!    destination permit, so a row waiting on a saturated destination holds no global permit and
//!    cannot starve the other destinations.
//!
//! Rows on a capacity-pooled channel do not enter chains: they go to one **pooled lane**, a single
//! task fed in claim order, so a free pool slot still goes to the oldest waiter. The claimer only
//! sends to it and never waits for a pooled send.
//!
//! Retry, backoff, poison and incident handling are the dispatcher's own per-row code, unchanged.
//! A chain that panics releases the rows it still held, so a later claim delivers them.
//!
//! [`submit`]: DeliveryScheduler::submit

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use sutra_executor::DeploymentId;
use time::OffsetDateTime;
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};

use crate::outbox_dispatch::{ClaimedOutboxRow, OutboxConcurrency, OutboxDispatcher, OutboxSignal};

/// One row waiting in a chain, with the buffer permit it holds until it leaves the scheduler.
struct Queued {
    deployment: DeploymentId,
    row: ClaimedOutboxRow,
    _buffer: OwnedSemaphorePermit,
}

/// A chain's identity: one process instance of one deployment.
type InstanceKey = (String, String);

/// The live counts the `sutra.outbox.in-flight` / `sutra.outbox.buffered` gauges read.
#[derive(Default)]
pub(crate) struct SchedulerGauges {
    in_flight: AtomicUsize,
    buffered: AtomicUsize,
}

impl SchedulerGauges {
    /// `(in flight, buffered)`: sends in progress, and rows held (in flight or queued).
    pub(crate) fn read(&self) -> (usize, usize) {
        (
            self.in_flight.load(Ordering::Relaxed),
            self.buffered.load(Ordering::Relaxed),
        )
    }
}

struct Inner {
    dispatcher: Arc<OutboxDispatcher>,
    signal: OutboxSignal,
    deletes: mpsc::UnboundedSender<(DeploymentId, ClaimedOutboxRow)>,
    rt: tokio::runtime::Handle,
    buffer: Arc<Semaphore>,
    global: Arc<Semaphore>,
    per_destination: usize,
    destinations: Mutex<HashMap<String, Arc<Semaphore>>>,
    chains: Mutex<HashMap<InstanceKey, VecDeque<Queued>>>,
    /// Notified whenever a chain ends, for [`DeliveryScheduler::close`].
    chain_ended: Notify,
    gauges: Arc<SchedulerGauges>,
}

/// Per-instance delivery chains under a buffer, a per-destination and a global bound (see the
/// module documentation).
pub(crate) struct DeliveryScheduler {
    inner: Arc<Inner>,
    pooled_tx: mpsc::UnboundedSender<(DeploymentId, ClaimedOutboxRow, OffsetDateTime)>,
    pooled_lane: tokio::task::JoinHandle<()>,
}

impl DeliveryScheduler {
    /// A scheduler delivering through `dispatcher`; delivered rows go to `deletes` (the delete
    /// batcher). Spawns the pooled lane on `rt`; chains are spawned there as rows arrive.
    pub(crate) fn new(
        dispatcher: Arc<OutboxDispatcher>,
        c: OutboxConcurrency,
        signal: OutboxSignal,
        deletes: mpsc::UnboundedSender<(DeploymentId, ClaimedOutboxRow)>,
        rt: tokio::runtime::Handle,
    ) -> DeliveryScheduler {
        let workers = c.workers.max(1);
        let per_destination = c.per_destination.clamp(1, workers);
        let max_buffered = c.max_buffered.max(1);
        let gauges = Arc::new(SchedulerGauges::default());
        let (pooled_tx, pooled_rx) = mpsc::unbounded_channel();
        let pooled_lane = rt.spawn(run_pooled_lane(
            Arc::clone(&dispatcher),
            signal.clone(),
            Arc::clone(&gauges),
            pooled_rx,
        ));
        DeliveryScheduler {
            inner: Arc::new(Inner {
                dispatcher,
                signal,
                deletes,
                rt,
                buffer: Arc::new(Semaphore::new(max_buffered)),
                global: Arc::new(Semaphore::new(workers)),
                per_destination,
                destinations: Mutex::new(HashMap::new()),
                chains: Mutex::new(HashMap::new()),
                chain_ended: Notify::new(),
                gauges,
            }),
            pooled_tx,
            pooled_lane,
        }
    }

    /// Take one claimed or handed-off row. Waits only for buffer room (`max_buffered`), never
    /// for a particular instance or destination.
    pub(crate) async fn submit(&self, deployment: DeploymentId, row: ClaimedOutboxRow) {
        let permit = Arc::clone(&self.inner.buffer)
            .acquire_owned()
            .await
            .expect("the buffer semaphore is never closed");
        self.inner.gauges.buffered.fetch_add(1, Ordering::Relaxed);
        let key = (deployment.value().to_string(), row.instance_id.clone());
        let queued = Queued {
            deployment,
            row,
            _buffer: permit,
        };
        let spawn = {
            let mut chains = self.inner.chains.lock().expect("chain lock");
            match chains.get_mut(&key) {
                Some(chain) => {
                    chain.push_back(queued);
                    false
                }
                None => {
                    chains.insert(key.clone(), VecDeque::from([queued]));
                    true
                }
            }
        };
        if spawn {
            let inner = Arc::clone(&self.inner);
            self.inner.rt.spawn(run_chain(inner, key));
        }
    }

    /// Hand a row of a capacity-pooled channel to the pooled lane, with the instant it was
    /// claimed at (a row that finds no slot goes back due one millisecond after it, keeping its
    /// place ahead of every later row). Never waits.
    pub(crate) fn submit_pooled(
        &self,
        deployment: DeploymentId,
        row: ClaimedOutboxRow,
        claimed_at: OffsetDateTime,
    ) {
        self.inner.gauges.buffered.fetch_add(1, Ordering::Relaxed);
        if let Err(returned) = self.pooled_tx.send((deployment, row, claimed_at)) {
            // The lane is gone (it only ends after close): release, a later claim delivers it.
            self.inner.gauges.buffered.fetch_sub(1, Ordering::Relaxed);
            self.inner.signal.release(&[returned.0 .1.entry_id]);
        }
    }

    /// The gauge cells, shared with the dispatcher handle.
    pub(crate) fn gauges(&self) -> Arc<SchedulerGauges> {
        Arc::clone(&self.inner.gauges)
    }

    /// Stop taking rows, then wait for the pooled lane and every chain to finish.
    pub(crate) async fn close(self) {
        let DeliveryScheduler {
            inner,
            pooled_tx,
            pooled_lane,
        } = self;
        drop(pooled_tx);
        let _ = pooled_lane.await;
        loop {
            let ended = inner.chain_ended.notified();
            tokio::pin!(ended);
            ended.as_mut().enable();
            if inner.chains.lock().expect("chain lock").is_empty() {
                break;
            }
            ended.await;
        }
    }
}

impl Inner {
    /// The bound of one destination authority, created on first use.
    fn destination(&self, authority: &str) -> Arc<Semaphore> {
        let mut map = self.destinations.lock().expect("destination lock");
        Arc::clone(
            map.entry(authority.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(self.per_destination))),
        )
    }

    /// Forget an authority's bound once nothing holds or waits on it (the map would otherwise
    /// grow with every destination ever seen).
    fn forget_destination_if_idle(&self, authority: &str) {
        let mut map = self.destinations.lock().expect("destination lock");
        if map
            .get(authority)
            .is_some_and(|sem| Arc::strong_count(sem) == 1)
        {
            map.remove(authority);
        }
    }

    /// The chain's next row, or `None` — and the chain removed — when it is empty.
    fn next_in_chain(&self, key: &InstanceKey) -> Option<Queued> {
        let mut chains = self.chains.lock().expect("chain lock");
        let chain = chains.get_mut(key)?;
        match chain.pop_front() {
            Some(next) => Some(next),
            None => {
                chains.remove(key);
                None
            }
        }
    }
}

/// Releases what a chain still holds if its task ends abnormally (a panicking send, or the
/// runtime shutting down): the row being sent and every row queued behind it, so a later claim
/// picks them up.
struct ChainGuard {
    inner: Arc<Inner>,
    key: InstanceKey,
    current: Option<String>,
    finished: bool,
}

impl Drop for ChainGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut ids: Vec<String> = Vec::new();
        if let Some(current) = self.current.take() {
            self.inner.gauges.buffered.fetch_sub(1, Ordering::Relaxed);
            ids.push(current);
        }
        let queued = self
            .inner
            .chains
            .lock()
            .map(|mut chains| chains.remove(&self.key))
            .unwrap_or_default();
        if let Some(queued) = queued {
            self.inner
                .gauges
                .buffered
                .fetch_sub(queued.len(), Ordering::Relaxed);
            ids.extend(queued.into_iter().map(|q| q.row.entry_id));
        }
        self.inner.signal.release(&ids);
        self.inner.chain_ended.notify_waiters();
    }
}

/// One instance's chain: deliver its rows one at a time, in order, until it is empty.
async fn run_chain(inner: Arc<Inner>, key: InstanceKey) {
    let mut guard = ChainGuard {
        inner: Arc::clone(&inner),
        key: key.clone(),
        current: None,
        finished: false,
    };
    while let Some(Queued {
        deployment,
        row,
        _buffer,
    }) = inner.next_in_chain(&key)
    {
        guard.current = Some(row.entry_id.clone());
        let authority = crate::sink::destination_authority(&row.destination);
        let destination = inner.destination(&authority);
        let dest_permit = destination
            .acquire()
            .await
            .expect("destination semaphores are never closed");
        let global_permit = inner
            .global
            .acquire()
            .await
            .expect("the global semaphore is never closed");
        inner.gauges.in_flight.fetch_add(1, Ordering::Relaxed);
        let delivered = {
            // Decrement the gauge even if the send panics.
            struct InFlight<'a>(&'a AtomicUsize);
            impl Drop for InFlight<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::Relaxed);
                }
            }
            let _in_flight = InFlight(&inner.gauges.in_flight);
            inner.dispatcher.deliver(&deployment, &row).await
        };
        drop(global_permit);
        drop(dest_permit);
        drop(destination);
        inner.forget_destination_if_idle(&authority);
        if delivered {
            inner.signal.mark_delivered(&row.entry_id);
            if let Err(returned) = inner.deletes.send((deployment, row)) {
                // The batcher is gone (shutdown): delete in place.
                let (deployment, row) = returned.0;
                inner.dispatcher.finish_delivered(&deployment, &row).await;
                inner.signal.release(&[row.entry_id]);
            }
        } else {
            inner.signal.release(&[row.entry_id]);
        }
        guard.current = None;
        inner.gauges.buffered.fetch_sub(1, Ordering::Relaxed);
        drop(_buffer);
    }
    guard.finished = true;
    drop(guard);
    inner.chain_ended.notify_waiters();
}

/// The pooled lane: rows of capacity-pooled channels, one at a time in claim order, through the
/// dispatcher's pooled path. The pools found full are remembered per deployment until the lane
/// drains, so a slot freed meanwhile cannot go to a newer row ahead of an older one.
async fn run_pooled_lane(
    dispatcher: Arc<OutboxDispatcher>,
    signal: OutboxSignal,
    gauges: Arc<SchedulerGauges>,
    mut rx: mpsc::UnboundedReceiver<(DeploymentId, ClaimedOutboxRow, OffsetDateTime)>,
) {
    let mut exhausted: HashMap<String, BTreeSet<String>> = HashMap::new();
    while let Some(first) = rx.recv().await {
        let mut next = Some(first);
        while let Some((deployment, row, claimed_at)) = next {
            gauges.in_flight.fetch_add(1, Ordering::Relaxed);
            dispatcher
                .dispatch_one(
                    &deployment,
                    &row,
                    claimed_at,
                    exhausted.entry(deployment.value().to_string()).or_default(),
                )
                .await;
            gauges.in_flight.fetch_sub(1, Ordering::Relaxed);
            signal.release(&[row.entry_id]);
            gauges.buffered.fetch_sub(1, Ordering::Relaxed);
            next = rx.try_recv().ok();
        }
        exhausted.clear();
    }
}
