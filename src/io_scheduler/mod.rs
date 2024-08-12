use crate::io_scheduler::queue::{ActiveHandle, Queue};
use crate::io_scheduler::reaper::Reaper;
use crate::vfs::File;
use anyhow::{anyhow, bail, Result};
use bimap::BiHashMap;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{watch, Notify};

pub(crate) mod download;
mod queue;
mod reaper;
pub(crate) mod upload;

pub(crate) struct Scheduler<B: Backend> {
    backend: B,
    allow_prepare_existing: bool,
    shared_state: Arc<RwLock<SharedState<B>>>,
    reaper: Reaper<B>,
}

pub(self) struct SharedState<B: Backend> {
    active: HashMap<B::Key, Status<B>>,
    file_id_keys: BiHashMap<u64, B::Key>,
}

impl<B: Backend + 'static + Sync> Scheduler<B> {
    fn new(backend: B, allow_prepare_existing: bool) -> Self {
        let shared_state = Arc::new(RwLock::new(SharedState {
            active: HashMap::new(),
            file_id_keys: BiHashMap::new(),
        }));
        let reaper: Reaper<B> = Reaper::new(shared_state.clone());

        Self {
            backend,
            allow_prepare_existing,
            shared_state,
            reaper,
        }
    }

    pub async fn prepare(&self, key: &B::Key) -> Result<File> {
        loop {
            let (notify, fut) = {
                let active = &mut self.shared_state.write().active;
                match active.get(key) {
                    None => {
                        let notify_ready = Arc::new(Notify::new());
                        active.insert(key.clone(), Status::Preparing(notify_ready.clone()));
                        (notify_ready, Some(self.backend.begin(key)))
                    }
                    Some(Status::Preparing(notify)) => (notify.clone(), None),
                    Some(Status::Ready(entry)) => {
                        if self.allow_prepare_existing {
                            return Ok(entry.file.borrow().clone());
                        } else {
                            bail!("entry for key {:?} already exists", key);
                        }
                    }
                }
            };

            if let Some(fut) = fut {
                let res = fut.await;
                let mut shared_state = self.shared_state.write();
                notify.notify_waiters();
                return match res {
                    Ok(queue) => {
                        let file = queue.file().borrow().clone();
                        let entry = Entry {
                            expiration: queue.expiration(),
                            file: queue.file(),
                            queue: Arc::new(Mutex::new(queue)),
                        };
                        shared_state
                            .active
                            .insert(key.clone(), Status::Ready(entry));
                        let file_id_keys = &mut shared_state.file_id_keys;
                        let file_id = file.id();
                        file_id_keys.insert(file_id, key.clone());
                        // let the reaper know there's a new entry
                        self.reaper.notify();
                        Ok(file)
                    }
                    Err(err) => {
                        shared_state.active.remove(key);
                        Err(err)
                    }
                };
            }

            // wait for the existing entry to be processed
            // then try again
            notify.notified().await;
        }
    }

    pub async fn lease(&self, file_id: u64, offset: u64) -> Result<Lease<B>> {
        let queue = {
            let shared_state = self.shared_state.read();
            let key = shared_state
                .file_id_keys
                .get_by_left(&file_id)
                .ok_or_else(|| anyhow!("key for file_id {} not found", file_id))?;
            let entry = match shared_state.active.get(&key) {
                Some(Status::Ready(entry)) => entry,
                _ => {
                    bail!("unable to get ready entry for key {:?}", key);
                }
            };
            entry.queue.clone()
        };

        let active_handle = self.backend.lease(queue.clone(), offset).await?;
        Ok(Lease {
            queue,
            active_handle: Some(active_handle),
        })
    }

    pub async fn file_by_key(&self, key: &B::Key) -> Result<Option<File>> {
        loop {
            let notify = {
                match self.shared_state.read().active.get(key) {
                    Some(Status::Ready(entry)) => {
                        return Ok(Some(entry.file.borrow().clone()));
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
            self.shared_state
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

pub(crate) enum Status<B: Backend> {
    Preparing(Arc<Notify>),
    Ready(Entry<B>),
}

struct Entry<B: Backend> {
    queue: Arc<Mutex<Queue<B::Task>>>,
    file: watch::Receiver<File>,
    expiration: watch::Receiver<SystemTime>,
}

impl<B: Backend> Entry<B> {
    fn expiration(&self) -> watch::Receiver<SystemTime> {
        self.expiration.clone()
    }
}

pub(crate) struct Lease<B: Backend> {
    queue: Arc<Mutex<Queue<B::Task>>>,
    active_handle: Option<ActiveHandle<B::Task>>,
}

impl<B: Backend> Drop for Lease<B> {
    fn drop(&mut self) {
        if let Some(handle) = self.active_handle.take() {
            let mut queue = self.queue.lock();
            queue.return_handle(handle);
        }
    }
}

impl<B: Backend> Deref for Lease<B> {
    type Target = B::Task;

    fn deref(&self) -> &Self::Target {
        self.active_handle.as_ref().unwrap().as_ref()
    }
}

impl<B: Backend> DerefMut for Lease<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.active_handle.as_mut().unwrap().as_mut()
    }
}

pub(crate) trait Backend: Sized {
    type Task: BackendTask;
    type Key: Hash + Eq + Clone + Send + 'static + Sync + Debug;

    fn begin(&self, key: &Self::Key) -> impl Future<Output = Result<Queue<Self::Task>>> + Send;
    async fn lease(
        &self,
        entry: Arc<Mutex<Queue<Self::Task>>>,
        offset: u64,
    ) -> Result<ActiveHandle<Self::Task>>;
}

pub(crate) trait BackendTask: Send {
    fn offset(&self) -> u64;
    fn can_reuse(&self) -> bool;
    fn finalize(self) -> impl Future<Output = Result<()>> + Send;
    fn to_file(&self) -> File;
}
