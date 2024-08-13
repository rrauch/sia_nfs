use crate::io_scheduler::queue::{ActiveHandle, Queue};
use crate::io_scheduler::reaper::Reaper;
use crate::vfs::File;
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
    queues: HashMap<B::Key, Status<B>>,
    file_id_keys: BiHashMap<u64, B::Key>,
}

impl<B: Backend + 'static + Sync> Scheduler<B> {
    fn new(backend: B, allow_existing_queue: bool) -> Self {
        let state = Arc::new(RwLock::new(State {
            queues: HashMap::new(),
            file_id_keys: BiHashMap::new(),
        }));
        let reaper: Reaper<B> = Reaper::new(state.clone());

        Self {
            backend,
            allow_existing_queue,
            state,
            reaper,
        }
    }

    pub async fn prepare(&self, key: &B::Key) -> Result<File> {
        loop {
            let (notify, fut) = {
                let queues = &mut self.state.write().queues;
                match queues.get(key) {
                    None => {
                        let notify_ready = Arc::new(Notify::new());
                        queues.insert(key.clone(), Status::Preparing(notify_ready.clone()));
                        (notify_ready, Some(self.backend.begin(key)))
                    }
                    Some(Status::Preparing(notify)) => (notify.clone(), None),
                    Some(Status::Ready(queue)) => {
                        if self.allow_existing_queue {
                            return Ok(queue.file().borrow().clone());
                        } else {
                            bail!("queue for key {:?} already exists", key);
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
                        let file = queue.file().borrow().clone();
                        state
                            .queues
                            .insert(key.clone(), Status::Ready(Arc::new(queue)));
                        let file_id_keys = &mut state.file_id_keys;
                        let file_id = file.id();
                        file_id_keys.insert(file_id, key.clone());
                        // let the reaper know there's a new queue
                        self.reaper.notify();
                        Ok(file)
                    }
                    Err(err) => {
                        state.queues.remove(key);
                        Err(err)
                    }
                };
            }

            // wait for the existing entry to be processed
            // then try again
            notify.notified().await;
        }
    }

    pub async fn acquire(&self, file_id: u64, offset: u64) -> Result<ActiveHandle<B::Task>> {
        let queue = {
            let state = self.state.read();
            let key = state
                .file_id_keys
                .get_by_left(&file_id)
                .ok_or_else(|| anyhow!("key for file_id {} not found", file_id))?;
            let queue = match state.queues.get(&key) {
                Some(Status::Ready(queue)) => queue,
                _ => {
                    bail!("unable to get queue for key {:?}", key);
                }
            };
            queue.clone()
        };

        Ok(self.backend.acquire(queue, offset).await?)
    }

    pub async fn file_by_key(&self, key: &B::Key) -> Result<Option<File>> {
        loop {
            let notify = {
                match self.state.read().queues.get(key) {
                    Some(Status::Ready(queue)) => {
                        return Ok(Some(queue.file().borrow().clone()));
                    }
                    Some(Status::Preparing(notify)) => notify.clone(),
                    None => return Ok(None),
                }
            };
            notify.notified().await;
        }
    }

    pub async fn file_by_id(&self, file_id: u64) -> Result<Option<File>> {
        let key = {
            self.state
                .read()
                .file_id_keys
                .get_by_left(&file_id)
                .map(|k| k.clone())
        };

        if let Some(key) = key {
            return self.file_by_key(&key).await;
        }
        Ok(None)
    }
}

enum Status<B: Backend> {
    Preparing(Arc<Notify>),
    Ready(Arc<Queue<B::Task>>),
}

pub(crate) trait Backend: Sized {
    type Task: BackendTask;
    type Key: Hash + Eq + Clone + Send + 'static + Sync + Debug;

    fn begin(&self, key: &Self::Key) -> impl Future<Output = Result<Queue<Self::Task>>> + Send;

    #[allow(private_interfaces)]
    async fn acquire(
        &self,
        queue: Arc<Queue<Self::Task>>,
        offset: u64,
    ) -> Result<ActiveHandle<Self::Task>>;
}

pub(crate) trait BackendTask: Send {
    fn offset(&self) -> u64;
    fn can_reuse(&self) -> bool;
    fn finalize(self) -> impl Future<Output = Result<()>> + Send;
    fn to_file(&self) -> File;
}
