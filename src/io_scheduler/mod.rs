use crate::io_scheduler::queue::{ActiveHandle, Queue};
use crate::io_scheduler::reaper::Reaper;
use anyhow::{anyhow, bail, Result};
use bimap::BiHashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::Notify;

pub(crate) mod download;
mod queue;
mod reaper;
pub(crate) mod upload;

pub(crate) struct Scheduler<B: Backend> {
    backend: B,
    allow_existing_queue: bool,
    state: Arc<RwLock<State<B>>>,
    reaper: Reaper<B>,
}

struct State<B: Backend> {
    queues: HashMap<B::PreparationKey, Status<B>>,
    key_map: BiHashMap<B::PreparationKey, B::AccessKey>,
}

impl<B: Backend + 'static + Sync> Scheduler<B> {
    fn new(backend: B, allow_existing_queue: bool) -> Self {
        let state = Arc::new(RwLock::new(State {
            queues: HashMap::new(),
            key_map: BiHashMap::new(),
        }));
        let reaper: Reaper<B> = Reaper::new(state.clone());

        Self {
            backend,
            allow_existing_queue,
            state,
            reaper,
        }
    }

    pub async fn prepare(&self, preparation_key: &B::PreparationKey) -> Result<B::AccessKey> {
        loop {
            let (notify, fut) = {
                let queues = &mut self.state.write().queues;
                match queues.get(preparation_key) {
                    None => {
                        let notify_ready = Arc::new(Notify::new());
                        queues.insert(
                            preparation_key.clone(),
                            Status::Preparing(notify_ready.clone()),
                        );
                        (notify_ready, Some(self.backend.prepare(preparation_key)))
                    }
                    Some(Status::Preparing(notify)) => (notify.clone(), None),
                    Some(Status::Ready(queue)) => {
                        if self.allow_existing_queue {
                            return Ok(queue.access_key());
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
                    Ok(queue) => {
                        let access_key = queue.access_key();
                        state
                            .queues
                            .insert(preparation_key.clone(), Status::Ready(Arc::new(queue)));
                        let key_map = &mut state.key_map;
                        key_map.insert(preparation_key.clone(), access_key.clone());
                        // let the reaper know there's a new queue
                        self.reaper.notify();
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

    pub async fn access(
        &self,
        access_key: &B::AccessKey,
        offset: u64,
    ) -> Result<ActiveHandle<B::Task>> {
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

        Ok(self.backend.access(queue, offset).await?)
    }
}

enum Status<B: Backend> {
    Preparing(Arc<Notify>),
    Ready(Arc<Queue<B::AccessKey, B::Task>>),
}

pub(crate) trait Backend: Sized {
    type Task: BackendTask;
    type PreparationKey: Hash + Eq + Clone + Send + 'static + Sync + Debug;
    type AccessKey: Hash + Eq + Clone + Send + 'static + Sync + Debug;

    fn prepare(
        &self,
        preparation_key: &Self::PreparationKey,
    ) -> impl Future<Output = Result<Queue<Self::AccessKey, Self::Task>>> + Send;

    #[allow(private_interfaces)]
    async fn access(
        &self,
        queue: Arc<Queue<Self::AccessKey, Self::Task>>,
        offset: u64,
    ) -> Result<ActiveHandle<Self::Task>>;
}

pub(crate) trait BackendTask: Send {
    fn offset(&self) -> u64;
    fn can_reuse(&self) -> bool;
    fn finalize(self) -> impl Future<Output = Result<()>> + Send;
}
