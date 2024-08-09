use crate::vfs::File;
use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwap;
use futures_util::stream::FuturesUnordered;
use futures_util::FutureExt;
use futures_util::StreamExt;
use itertools::{Either, Itertools};
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout;

pub(crate) mod download;
pub(crate) mod upload;

pub(crate) struct Scheduler<B: Backend> {
    backend: B,
    active: Arc<
        Mutex<
            HashMap<
                B::Key,
                (
                    Arc<ArcSwap<SystemTime>>,
                    watch::Receiver<Status>,
                    Arc<Mutex<Entry<B>>>,
                ),
            >,
        >,
    >,
    max_idle: Duration,
    allow_prepare_existing: bool,
    notify_reaper: Arc<Notify>,
    reaper: JoinHandle<()>,
}

impl<B: Backend + 'static + Sync> Scheduler<B> {
    fn new(backend: B, max_idle: Duration, allow_prepare_existing: bool) -> Self {
        let notify_reaper = Arc::new(Notify::new());
        let active = Arc::new(Mutex::new(HashMap::new()));

        let reaper = {
            let notify_reaper = notify_reaper.clone();
            let active = active.clone();
            tokio::spawn(async move {
                Self::reap(notify_reaper, active).await;
            })
        };

        Self {
            backend,
            max_idle,
            allow_prepare_existing,
            active,
            notify_reaper,
            reaper,
        }
    }

    async fn reap(
        notify_reaper: Arc<Notify>,
        active: Arc<
            Mutex<
                HashMap<
                    B::Key,
                    (
                        Arc<ArcSwap<SystemTime>>,
                        watch::Receiver<Status>,
                        Arc<Mutex<Entry<B>>>,
                    ),
                >,
            >,
        >,
    ) {
        loop {
            let mut next_check = SystemTime::now() + Duration::from_secs(60);
            let mut finalizations =
                {
                    let mut lock = active.lock().unwrap();
                    let now = SystemTime::now();

                    // once `extract_if` has been stabilized this can be used instead of the following code
                    // see https://github.com/rust-lang/rust/issues/59618
                    let keys = lock
                        .iter()
                        .filter_map(|(k, (t, s, e))| {
                            if t.load().as_ref() <= &now {
                                // entry is expired
                                if let Status::Ready(_) = s.borrow().deref() {
                                    if Arc::strong_count(e) == 1 {
                                        // no one else is holding onto this, ready for reaping
                                        return Some(k.clone());
                                    }
                                }
                            }
                            None
                        })
                        .collect::<Vec<_>>();

                    keys.into_iter().filter_map(|key| {
                    if let Some((_, _, e)) = lock.remove(&key) {
                        if let Some(entry) = Arc::into_inner(e) {
                            if let Ok(entry) = entry.into_inner() {
                                return Some(entry.finalize());
                            }
                        }
                    }
                    tracing::error!(key = ?key, "unable to properly finalize expired entry");
                    None
                }).collect::<FuturesUnordered<_>>()
                };

            // awaiting all finalizations
            while let Some(_) = finalizations.next().await {}

            // update `next_check`
            {
                let lock = active.lock().unwrap();
                for (_, (t, _, _)) in lock.iter() {
                    let expiration = t.load();
                    if expiration.as_ref() < &next_check {
                        next_check = *expiration.as_ref();
                    }
                }
            }

            let sleep_duration = next_check
                .duration_since(SystemTime::now())
                .unwrap_or(Duration::from_secs(0));

            // make sure we don't reap again right away (unless we are notified)
            let sleep_duration = max(sleep_duration, Duration::from_millis(100));

            tokio::select! {
                _ = tokio::time::sleep(sleep_duration) => {},
                _ = notify_reaper.notified() => {}
            }
        }
    }

    pub async fn prepare(&self, key: &B::Key) -> anyhow::Result<File> {
        let fut = {
            let mut lock = self.active.lock().expect("unable to acquire active lock");
            if lock.contains_key(key) {
                if !self.allow_prepare_existing {
                    bail!("unable to prepare - already exists");
                }
                let mut status_rx = lock.get(key).unwrap().1.clone();

                async move {
                    loop {
                        if let Status::Ready(file) = status_rx.borrow_and_update().deref() {
                            return Ok::<File, anyhow::Error>(file.clone());
                        }
                        status_rx.changed().await?;
                    }
                }
                .boxed()
            } else {
                let (status_tx, status_rx) = watch::channel(Status::Preparing);
                let expiration = Arc::new(ArcSwap::from_pointee(
                    SystemTime::now() + Duration::from_secs(60),
                ));
                let entry = Arc::new(Mutex::new(Entry {
                    key: key.clone(),
                    tasks: vec![],
                    max_idle: self.max_idle,
                    status_tx: watch::channel(TasksStatus {
                        activity: Activity::Idle(None),
                        idle_offsets: vec![],
                        active_offsets: vec![],
                        reserved_offsets: vec![],
                    })
                    .0,
                    next_expiration: expiration.clone(),
                    notify_reaper: self.notify_reaper.clone(),
                }));
                lock.insert(key.clone(), (expiration, status_rx, entry.clone()));
                {
                    let backend = &self.backend;
                    let map = self.active.clone();
                    async move {
                        let (file, tasks) = match backend.begin(key).await {
                            Ok((file, tasks)) => (file, tasks),
                            Err(err) => {
                                // failed to prepare new entry
                                let mut lock = map.lock().unwrap();
                                lock.remove(key);
                                tracing::error!("backend begin failed: {}, entry removed", err);
                                return Err(err);
                            }
                        };
                        {
                            let mut lock = entry.lock().expect("unable to get entry lock");
                            lock.insert_tasks(tasks);
                        }
                        let _ = status_tx.send(Status::Ready(file.clone()));
                        Ok::<File, anyhow::Error>(file)
                    }
                    .boxed()
                }
            }
        };
        Ok(fut.await?)
    }

    pub async fn lease(&self, file_id: u64, offset: u64) -> Result<Lease<B>> {
        let entry = {
            let mut lock = self.active.lock().unwrap();
            Self::entry(&mut lock, Either::Left(file_id)).map(|(_, e)| e.clone())
        }
        .ok_or_else(|| anyhow!("unable to find entry for file_id {}", file_id))?;
        let backend = &self.backend;
        Ok(timeout(Duration::from_secs(60), backend.lease(entry, offset)).await??)
    }

    pub async fn file_by_key(&self, key: &B::Key) -> Result<Option<File>> {
        self.file(Either::Right(key)).await
    }

    pub async fn file_by_id(&self, file_id: u64) -> Result<Option<File>> {
        self.file(Either::Left(file_id)).await
    }

    fn entry<'a>(
        map: &'a HashMap<
            B::Key,
            (
                Arc<ArcSwap<SystemTime>>,
                watch::Receiver<Status>,
                Arc<Mutex<Entry<B>>>,
            ),
        >,
        option: Either<u64, &B::Key>,
    ) -> Option<(&'a watch::Receiver<Status>, &'a Arc<Mutex<Entry<B>>>)> {
        match option {
            Either::Right(key) => map.get(key).map(|(_, rx, e)| (rx, e)),
            Either::Left(id) => map.values().find_map(|(_, rx, e)| {
                if let Status::Ready(file) = rx.borrow().deref() {
                    if file.id() == id {
                        Some((rx, e))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }),
        }
    }

    async fn file(&self, option: Either<u64, &B::Key>) -> Result<Option<File>> {
        let mut rx = {
            let mut lock = self.active.lock().unwrap();
            let Some((rx, _)) = Self::entry(&mut lock, option) else {
                return Ok(None);
            };
            rx.clone()
        };
        loop {
            if let Status::Ready(file) = rx.borrow_and_update().deref() {
                return Ok(Some(file.clone()));
            }
            rx.changed().await?;
        }
    }
}

impl<B: Backend> Drop for Scheduler<B> {
    fn drop(&mut self) {
        self.reaper.abort();
    }
}

pub(crate) struct Entry<B: Backend> {
    key: B::Key,
    tasks: Vec<Task<B::Task>>,
    max_idle: Duration,
    status_tx: watch::Sender<TasksStatus>,
    next_expiration: Arc<ArcSwap<SystemTime>>,
    notify_reaper: Arc<Notify>,
}

#[derive(Clone, Debug)]
enum Activity {
    Active,
    Idle(Option<SystemTime>),
}

#[derive(Clone, Debug)]
struct TasksStatus {
    activity: Activity,
    idle_offsets: Vec<u64>,
    active_offsets: Vec<u64>,
    reserved_offsets: Vec<u64>,
}

#[derive(PartialEq, Clone)]
pub(crate) enum Status {
    Preparing,
    Ready(File),
}

impl<B: Backend> Entry<B> {
    async fn finalize(self) {
        let mut finalizers = self
            .tasks
            .into_iter()
            .filter_map(|t| match t {
                Task::Idle { task, .. } => Some(task.finalize()),
                _ => None,
            })
            .collect::<FuturesUnordered<_>>();

        while let Some(res) = finalizers.next().await {
            if let Err(err) = res {
                tracing::error!(error = %err, "error finalizing backend task");
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    fn status(&self) -> TasksStatus {
        let activity = {
            if self
                .tasks
                .iter()
                .find(|t| match t {
                    Task::Active { .. } => true,
                    Task::Reserved { .. } => true,
                    _ => false,
                })
                .is_some()
            {
                // currently active
                Activity::Active
            } else {
                let last_idle = self
                    .tasks
                    .iter()
                    .filter_map(|t| match t {
                        Task::Idle { idle_since, .. } => Some(idle_since),
                        _ => None,
                    })
                    .max()
                    .map(|t| *t);

                Activity::Idle(last_idle)
            }
        };

        let mut idle_offsets = vec![];
        let mut active_offsets = vec![];
        let mut reserved_offsets = vec![];

        self.tasks.iter().for_each(|t| match t {
            Task::Idle { offset, .. } => idle_offsets.push(*offset),
            Task::Reserved { offset, .. } => reserved_offsets.push(*offset),
            Task::Active { initial_offset, .. } => active_offsets.push(*initial_offset),
        });

        TasksStatus {
            activity,
            idle_offsets,
            active_offsets,
            reserved_offsets,
        }
    }

    fn remove_expired(&mut self) -> Vec<B::Task> {
        let now = SystemTime::now();
        let mut expired = vec![];
        self.tasks = std::mem::take(&mut self.tasks)
            .into_iter()
            .filter_map(|t| {
                if t.is_idle() && t.expiration() <= now {
                    match t {
                        Task::Idle { task, .. } => expired.push(task),
                        _ => unreachable!("task is not idle!"),
                    }
                    None
                } else {
                    Some(t)
                }
            })
            .collect::<Vec<_>>();

        if !expired.is_empty() {
            let _ = self.status_tx.send(self.status());
            self.update_expiration();
        }

        expired
    }

    fn update_expiration(&mut self) {
        let next_expiration = self
            .tasks
            .iter()
            .min_by_key(|t| t.expiration())
            .map(|t| t.expiration())
            .unwrap_or_else(|| SystemTime::now());

        if self.next_expiration.load().as_ref() != &next_expiration {
            self.next_expiration.store(Arc::new(next_expiration));
            self.notify_reaper.notify_one();
        }
    }

    fn return_lease(&mut self, initial_offset: u64, task: B::Task) {
        let end_offset = task.offset();
        let bytes_processed = task.offset() - initial_offset;
        match self.tasks.iter_mut().find(|task| match task {
            Task::Active {
                initial_offset: task_offset,
                ..
            } => task_offset == &initial_offset,
            _ => false,
        }) {
            Some(t) => {
                *t = Task::Idle {
                    offset: task.offset(),
                    expiration: SystemTime::now() + self.max_idle,
                    idle_since: SystemTime::now(),
                    task,
                }
            }
            None => {
                tracing::error!("unable to return lease {}", initial_offset);
            }
        }
        tracing::trace!(key = ?self.key, initial_offset, end_offset, bytes_processed, "lease returned");
        self.update_expiration();
        let _ = self.status_tx.send(self.status());
    }

    fn insert_tasks(&mut self, tasks: Vec<B::Task>) {
        for task in tasks {
            self.tasks.push(Task::Idle {
                offset: task.offset(),
                expiration: SystemTime::now() + self.max_idle,
                idle_since: SystemTime::now(),
                task,
            });
            let _ = self.status_tx.send(self.status());
        }
        self.update_expiration();
    }

    fn make_reservation(&mut self, entry: &Arc<Mutex<Entry<B>>>, offset: u64) -> Reservation<B> {
        self.tasks.push(Task::Reserved { offset });
        let _ = self.status_tx.send(self.status());
        tracing::trace!(key = ?self.key, offset = offset, "new reservation");
        Reservation {
            return_on_drop: true,
            entry: entry.clone(),
            offset,
        }
    }

    fn redeem_reservation(
        &mut self,
        offset: u64,
        entry: &Arc<Mutex<Entry<B>>>,
        task: B::Task,
    ) -> Result<Lease<B>> {
        match self.tasks.iter_mut().find(|task| match task {
            Task::Reserved {
                offset: task_offset,
                ..
            } => &offset == task_offset,
            _ => false,
        }) {
            Some(t) => {
                *t = Task::Active {
                    initial_offset: task.offset(),
                }
            }
            None => {
                bail!("reservation not found");
            }
        }
        tracing::trace!(key = ?self.key, offset, "reservation redeemed");
        Ok(Lease {
            entry: entry.clone(),
            initial_offset: offset,
            backend_task: Some(task),
        })
    }

    fn return_reservation(&mut self, offset: u64) {
        let mut found = false;
        self.tasks = std::mem::take(&mut self.tasks)
            .into_iter()
            .filter(|t| {
                if let Task::Reserved {
                    offset: task_offset,
                    ..
                } = t
                {
                    if task_offset == &offset {
                        found = true;
                        false
                    } else {
                        true
                    }
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();
        tracing::trace!(key = ?self.key, offset, "unused reservation returned");
        if found {
            let _ = self.status_tx.send(self.status());
        }
    }

    /// Attempts to remove the oldest idle task
    fn free_task(&mut self) -> Option<B::Task> {
        if let Some((index, _)) = self
            .tasks
            .iter()
            .enumerate()
            .filter_map(|(index, task)| {
                if let Task::Idle { idle_since, .. } = task {
                    Some((index, idle_since))
                } else {
                    None
                }
            })
            .min_by_key(|&(_, idle_since)| idle_since)
        {
            Some(match self.tasks.swap_remove(index) {
                Task::Idle { task, .. } => {
                    let _ = self.status_tx.send(self.status());
                    tracing::trace!("task {} freed", task.offset());
                    task
                }
                _ => unreachable!(),
            })
        } else {
            None
        }
    }

    fn lease(&mut self, entry: &Arc<Mutex<Entry<B>>>, offset: u64) -> Option<Lease<B>> {
        let wanted_offset = offset;

        let (i, _) = self.tasks.iter().find_position(|t| match t {
            Task::Idle { offset, .. } => offset == &wanted_offset,
            _ => false,
        })?;

        match std::mem::replace(
            &mut self.tasks[i],
            Task::Active {
                initial_offset: offset,
            },
        ) {
            Task::Idle { offset, task, .. } => {
                if offset != wanted_offset {
                    panic!("invalid offset in task: {} != {}", wanted_offset, offset);
                }
                tracing::trace!(key = ?self.key, offset, "new lease");
                let _ = self.status_tx.send(self.status());
                self.update_expiration();
                Some(Lease {
                    entry: entry.clone(),
                    initial_offset: offset,
                    backend_task: Some(task),
                })
            }
            _ => {
                unreachable!("replaced task is wrong");
            }
        }
    }
}

enum Task<BT: BackendTask> {
    Idle {
        offset: u64,
        expiration: SystemTime,
        idle_since: SystemTime,
        task: BT,
    },
    Active {
        initial_offset: u64,
    },
    Reserved {
        offset: u64,
    },
}

impl<BT: BackendTask> Task<BT> {
    fn expiration(&self) -> SystemTime {
        match &self {
            Task::Idle { expiration, .. } => expiration.clone(),
            _ => SystemTime::now() + Duration::from_secs(3600),
        }
    }

    fn is_idle(&self) -> bool {
        match &self {
            Task::Idle { .. } => true,
            _ => false,
        }
    }
}

struct Reservation<B: Backend> {
    entry: Arc<Mutex<Entry<B>>>,
    offset: u64,
    return_on_drop: bool,
}

impl<B: Backend> Reservation<B> {
    pub fn redeem(mut self, task: B::Task) -> Result<Lease<B>> {
        self.return_on_drop = false;
        let mut lock = self.entry.lock().unwrap();
        lock.redeem_reservation(self.offset, &self.entry, task)
    }
}

impl<B: Backend> Drop for Reservation<B> {
    fn drop(&mut self) {
        if self.return_on_drop {
            let mut lock = self.entry.lock().unwrap();
            lock.return_reservation(self.offset);
        }
    }
}

pub(crate) struct Lease<B: Backend> {
    entry: Arc<Mutex<Entry<B>>>,
    initial_offset: u64,
    backend_task: Option<B::Task>,
}

impl<B: Backend> Drop for Lease<B> {
    fn drop(&mut self) {
        if let Some(task) = self.backend_task.take() {
            let mut lock = self.entry.lock().unwrap();
            lock.return_lease(self.initial_offset, task);
        }
    }
}

impl<B: Backend> Deref for Lease<B> {
    type Target = B::Task;

    fn deref(&self) -> &Self::Target {
        &self.backend_task.as_ref().unwrap()
    }
}

impl<B: Backend> DerefMut for Lease<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.backend_task.as_mut().unwrap()
    }
}

pub(crate) trait Backend: Sized {
    type Task: BackendTask;
    type Key: Hash + Eq + Clone + Send + 'static + Sync + Debug;

    fn begin(
        &self,
        key: &Self::Key,
    ) -> impl std::future::Future<Output = Result<(File, Vec<Self::Task>)>> + Send;
    async fn lease(&self, entry: Arc<Mutex<Entry<Self>>>, offset: u64) -> Result<Lease<Self>>;
}

pub(crate) trait BackendTask: Send {
    fn offset(&self) -> u64;
    fn finalize(self) -> impl std::future::Future<Output = Result<()>> + Send;
}
