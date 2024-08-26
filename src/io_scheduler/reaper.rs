use crate::io_scheduler::queue::Queue;
use crate::io_scheduler::{Backend, BackendTask, State, Status};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use itertools::Itertools;
use parking_lot::RwLock;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::instrument;

pub(super) struct Reaper<B: Backend> {
    notify_tx: watch::Sender<()>,
    _handle: JoinHandle<()>,
    _phantom_data: PhantomData<B>,
}

impl<B: Backend + 'static> Reaper<B> {
    pub fn new(shared_state: Arc<RwLock<State<B>>>) -> Self {
        let (notify_tx, notify_rx) = watch::channel(());
        let mut runner = Runner {
            shared_state,
            notify_rx,
            expiration_tracker: ExpirationTracker {
                map: HashMap::new(),
                expired: HashSet::new(),
                next_expiration: None,
            },
        };

        let handle = tokio::task::spawn(async move {
            runner.run().await;
        });

        Self {
            notify_tx,
            _handle: handle,
            _phantom_data: PhantomData::default(),
        }
    }

    pub fn notify(&self) {
        let _ = self.notify_tx.send(());
    }
}

impl<B: Backend> Drop for Reaper<B> {
    fn drop(&mut self) {
        self._handle.abort();
    }
}

struct Runner<B: Backend> {
    shared_state: Arc<RwLock<State<B>>>,
    notify_rx: watch::Receiver<()>,
    expiration_tracker: ExpirationTracker<B>,
}

fn sync<B: Backend>(expiration_tracker: &mut ExpirationTracker<B>, shared_state: &State<B>) {
    expiration_tracker.sync(shared_state.queues.iter().filter_map(|(k, v)| {
        if let Status::Ready(entry) = v {
            Some((k, entry))
        } else {
            None
        }
    }));
}

impl<B: Backend> Runner<B> {
    async fn run(&mut self) {
        // sync expiration tracker
        {
            let shared_state = self.shared_state.read();
            sync(&mut self.expiration_tracker, &shared_state);
        }

        loop {
            let mut finalizations = FuturesUnordered::new();
            {
                let mut shared_state = self.shared_state.write();
                self.expiration_tracker
                    .expired
                    .iter()
                    .for_each(|preparation_key| {
                        let mut reap = false;
                        if let Some(Status::Ready(queue)) = shared_state.queues.get(preparation_key)
                        {
                            {
                                let mut guard = queue.lock();
                                guard
                                    .remove_expired_idle()
                                    .into_iter()
                                    .for_each(|t| finalizations.push(t.finalize()));
                                reap = queue.is_empty();
                            };

                            if reap {
                                if Arc::strong_count(queue) != 1 {
                                    tracing::debug!(
                                        "queue {:?} empty but still held, postponing removal",
                                        preparation_key
                                    );
                                    reap = false;
                                }
                            }
                        }
                        if reap {
                            shared_state.queues.remove(preparation_key);
                            shared_state.key_map.remove_by_left(preparation_key);
                            tracing::debug!("removed expired queue {:?}", preparation_key);
                        }
                    })
            }

            // awaiting all finalizations
            if !finalizations.is_empty() {
                tracing::debug!("removed {} expired tasks", finalizations.len());
            }
            while let Some(_) = finalizations.next().await {}

            // sync expiration tracker
            {
                let shared_state = self.shared_state.read();
                sync(&mut self.expiration_tracker, &shared_state);
                // reset notifications
                self.notify_rx.mark_unchanged();
            }

            // wait until either the next expiration or until a new queue has been added
            tokio::select! {
                _ = self.expiration_tracker.wait_for_next_expiration(Duration::from_millis(50)) => {},
                _ = self.notify_rx.changed() => {}
            }
        }
    }
}

struct ExpirationTracker<B: Backend> {
    map: HashMap<B::PreparationKey, watch::Receiver<SystemTime>>,
    expired: HashSet<B::PreparationKey>,
    next_expiration: Option<SystemTime>,
}

impl<B: Backend> ExpirationTracker<B> {
    fn sync<'a, I>(&mut self, active: I)
    where
        I: Iterator<
            Item = (
                &'a B::PreparationKey,
                &'a Arc<Queue<B::AccessKey, B::Task, B::Data>>,
            ),
        >,
        B: 'a,
    {
        let mut seen_keys = HashSet::new();

        active.for_each(|(k, v)| {
            if !self.map.contains_key(k) {
                self.map.insert(k.clone(), v.expiration());
            }
            seen_keys.insert(k);
        });

        self.map.retain(|k, _| seen_keys.contains(k));
        self.refresh();
    }

    #[instrument(skip(self))]
    fn refresh(&mut self) {
        let now = SystemTime::now();
        self.expired.clear();
        self.next_expiration = self
            .map
            .iter_mut()
            .map(|(k, w)| {
                let expiration = *w.borrow_and_update();
                if expiration < now {
                    self.expired.insert(k.clone());
                }
                expiration
            })
            .min();
    }

    async fn wait_for_next_expiration(&mut self, min_wait: Duration) {
        let futures = self
            .map
            .iter_mut()
            .map(|(k, v)| {
                async {
                    while let Ok(()) = v.changed().await {
                        if v.borrow_and_update().deref() <= &SystemTime::now() {
                            break;
                        }
                    }
                    k.clone()
                }
                .boxed()
            })
            .collect_vec();

        if futures.is_empty() {
            // wait forever
            future::pending::<()>().await;
        } else {
            let sleep_duration = self
                .next_expiration
                .unwrap_or_else(|| SystemTime::now() + Duration::from_secs(86400))
                .duration_since(SystemTime::now())
                .unwrap_or(Duration::from_secs(0));

            // ensure we wait at least `min_wait` duration
            let effective_sleep_duration = max(sleep_duration, min_wait);

            tokio::select! {
                _ = tokio::time::sleep(effective_sleep_duration) => {},
                (expired_key, _, _) = futures::future::select_all(futures) => {
                    self.expired.insert(expired_key);
                    self.next_expiration = Some(SystemTime::now());
                }
            }
        }
    }
}
