use crate::io_scheduler::queue::{ActiveHandle, Queue};
use anyhow::{anyhow, bail, Result};
use bimap::BiHashMap;
use futures_util::FutureExt;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tracing::instrument;

pub(crate) mod queue;
pub(crate) mod strategy;
pub(crate) mod upload;

pub(crate) struct Scheduler<RM: ResourceManager>
where
    <RM as ResourceManager>::Resource: 'static,
{
    resource_manager: Arc<RM>,
    allow_existing_queue: bool,
    max_queue_idle: Duration,
    max_resource_idle: Duration,
    max_prep_errors: usize,
    state: Arc<RwLock<State<RM>>>,
    term_tx: mpsc::Sender<RM::PreparationKey>,
    _reaper: JoinHandle<()>,
}

struct State<RM: ResourceManager>
where
    <RM as ResourceManager>::Resource: 'static,
{
    queues: HashMap<RM::PreparationKey, Status<RM>>,
    key_map: BiHashMap<RM::PreparationKey, RM::AccessKey>,
}

impl<RM: ResourceManager + 'static + Send + Sync> Scheduler<RM> {
    pub(crate) fn new(
        resource_manager: RM,
        allow_existing_queue: bool,
        max_queue_idle: Duration,
        max_resource_idle: Duration,
        max_prep_errors: usize,
    ) -> Self {
        let state = Arc::new(RwLock::new(State {
            queues: HashMap::new(),
            key_map: BiHashMap::new(),
        }));

        let (term_tx, mut term_rx) = mpsc::channel(10);

        let _reaper = {
            let state = state.clone();
            tokio::spawn(async move {
                while let Some(preparation_key) = term_rx.recv().await {
                    let queue = {
                        let state = &mut state.write();
                        state.key_map.remove_by_left(&preparation_key);
                        if let Some(Status::Ready(queue)) = state.queues.remove(&preparation_key) {
                            Some(queue)
                        } else {
                            None
                        }
                    };
                    if let Some(mut queue) = queue {
                        tokio::spawn(async move {
                            tracing::debug!("shutting down queue {:?}", preparation_key);
                            let queue = loop {
                                match Arc::try_unwrap(queue) {
                                    Ok(queue) => break queue,
                                    Err(arc_queue) => {
                                        queue = arc_queue;
                                        tokio::time::sleep(Duration::from_millis(250)).await;
                                    }
                                }
                            };
                            queue.shutdown().await;
                            tracing::trace!("queue {:?} shutdown complete", preparation_key);
                        });
                    }
                }
            })
        };

        Self {
            resource_manager: Arc::new(resource_manager),
            allow_existing_queue,
            max_queue_idle,
            max_resource_idle,
            max_prep_errors,
            state,
            term_tx,
            _reaper,
        }
    }

    pub async fn prepare(&self, preparation_key: &RM::PreparationKey) -> Result<RM::AccessKey> {
        loop {
            let (notify, fut) = {
                let state = &mut self.state.write();
                match state.queues.get(preparation_key) {
                    None => {
                        let notify_ready = Arc::new(Notify::new());
                        state.queues.insert(
                            preparation_key.clone(),
                            Status::Preparing(notify_ready.clone()),
                        );
                        (
                            notify_ready,
                            Some(self.resource_manager.prepare(preparation_key)),
                        )
                    }
                    Some(Status::Preparing(notify)) => (notify.clone(), None),
                    Some(Status::Ready(_)) => {
                        if self.allow_existing_queue {
                            return state
                                .key_map
                                .get_by_left(preparation_key)
                                .map(|k| k.clone())
                                .ok_or(anyhow!(
                                    "unable to find access key for preparation key {:?}",
                                    preparation_key
                                ));
                        } else {
                            bail!(
                                "queue for preparation_key {:?} already exists",
                                preparation_key
                            );
                        }
                    }
                }
            };

            if let Some(fut) = fut {
                let res = fut.await;
                let mut state = self.state.write();
                notify.notify_waiters();
                return match res {
                    Ok((access_key, resource_data, advise_data, initial_resources)) => {
                        let term_fn = {
                            let preparation_key = preparation_key.clone();
                            let term_tx = self.term_tx.clone();
                            || {
                                async move {
                                    if let Err(err) = term_tx.send(preparation_key).await {
                                        tracing::error!(error = %err, "error sending queue termination message to reaper")
                                    }
                                }
                                .boxed()
                            }
                        };

                        let queue = Queue::new(
                            self.resource_manager.clone(),
                            resource_data,
                            advise_data,
                            initial_resources,
                            self.max_queue_idle,
                            self.max_resource_idle,
                            self.max_prep_errors,
                            Some(term_fn),
                        );
                        state
                            .queues
                            .insert(preparation_key.clone(), Status::Ready(Arc::new(queue)));
                        let key_map = &mut state.key_map;
                        key_map.insert(preparation_key.clone(), access_key.clone());
                        Ok(access_key)
                    }
                    Err(err) => {
                        state.queues.remove(preparation_key);
                        Err(err)
                    }
                };
            }

            // wait for the existing entry to be processed
            // then try again
            notify.notified().await;
        }
    }

    #[instrument(skip(self))]
    pub async fn access(
        &self,
        access_key: &RM::AccessKey,
        offset: u64,
    ) -> Result<ActiveHandle<RM::Resource>> {
        tracing::trace!("access request");
        let queue = {
            let state = self.state.read();
            let preparation_key = state.key_map.get_by_right(access_key).ok_or_else(|| {
                anyhow!("preparation_key for access_key {:?} not found", access_key)
            })?;
            let queue = match state.queues.get(preparation_key) {
                Some(Status::Ready(queue)) => queue,
                _ => {
                    bail!(
                        "unable to get queue for preparation_key {:?}",
                        preparation_key
                    );
                }
            };
            queue.clone()
        };

        let resp = queue.access(offset).await;
        tracing::trace!(success = resp.is_ok(), "access response");
        Ok(resp?)
    }
}

enum Status<RM: ResourceManager>
where
    <RM as ResourceManager>::Resource: 'static,
{
    Preparing(Arc<Notify>),
    Ready(Arc<Queue<RM>>),
}

pub(crate) trait ResourceManager: Sized {
    type Resource: Resource;
    type PreparationKey: Hash + Eq + Clone + Send + 'static + Sync + Debug;
    type AccessKey: Hash + Eq + Clone + Send + 'static + Sync + Debug;
    type ResourceData: Send + Sync + 'static;
    type AdviseData: Send + Sync + 'static;

    fn prepare(
        &self,
        preparation_key: &Self::PreparationKey,
    ) -> impl Future<
        Output = Result<(
            Self::AccessKey,
            Self::ResourceData,
            Self::AdviseData,
            Vec<Self::Resource>,
        )>,
    > + Send;

    fn new_resource(
        &self,
        offset: u64,
        data: &Self::ResourceData,
    ) -> impl Future<Output = Result<Self::Resource>> + Send;

    fn advise<'a>(
        &self,
        state: &'a QueueState,
        data: &mut Self::AdviseData,
    ) -> Result<(Duration, Option<Action<'a>>)>;
}

pub(crate) trait Resource: Send {
    fn offset(&self) -> u64;
    fn can_reuse(&self) -> bool;
    fn finalize(self) -> impl Future<Output = Result<()>> + Send;
}

pub(crate) enum Action<'a> {
    Free(&'a Idle),
    NewResource(&'a Waiting),
}

pub(crate) struct QueueState {
    pub idle: Entries<Idle>,
    pub waiting: Entries<Waiting>,
    pub active: Entries<Active>,
    pub preparing: Entries<Preparing>,
}

impl QueueState {
    fn new(
        idle: Vec<Idle>,
        waiting: Vec<Waiting>,
        active: Vec<Active>,
        preparing: Vec<Preparing>,
    ) -> Self {
        Self {
            idle: idle.into_iter().map(|e| (e.offset, e)).collect(),
            waiting: waiting.into_iter().map(|e| (e.offset, e)).collect(),
            active: active.into_iter().map(|e| (e.offset, e)).collect(),
            preparing: preparing.into_iter().map(|e| (e.offset, e)).collect(),
        }
    }
}

pub(crate) struct Entries<T> {
    len: usize,
    map: BTreeMap<u64, Vec<T>>,
}

impl<T> FromIterator<(u64, T)> for Entries<T> {
    fn from_iter<I: IntoIterator<Item = (u64, T)>>(iter: I) -> Self {
        let mut map: BTreeMap<u64, Vec<T>> = BTreeMap::new();
        let mut len = 0;
        iter.into_iter().for_each(|(offset, v)| {
            map.entry(offset).or_default().push(v);
            len += 1;
        });
        Self { len, map }
    }
}

impl<T> Entries<T> {
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.map.values().flat_map(|t| t.iter())
    }

    pub fn get_before_offset(&self, offset: u64) -> impl Iterator<Item = &T> {
        self.map.range(..offset).flat_map(|(_, v)| v.iter())
    }

    pub fn get_at_or_after_offset(&self, offset: u64) -> impl Iterator<Item = &T> {
        self.map.range(offset..).flat_map(|(_, v)| v.iter())
    }
}

pub(crate) struct Idle {
    id: usize,
    pub offset: u64,
    pub since: SystemTime,
}

pub(crate) struct Waiting {
    id: usize,
    pub offset: u64,
    pub since: SystemTime,
}

pub(crate) struct Active {
    pub offset: u64,
}

pub(crate) struct Preparing {
    pub offset: u64,
}