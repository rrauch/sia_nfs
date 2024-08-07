use crate::vfs::{File, FileReader, FileWriter, Inode, Vfs};
use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwap;
use futures_util::stream::FuturesUnordered;
use futures_util::{AsyncSeekExt, FutureExt, StreamExt};
use itertools::{Either, Itertools};
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::time::timeout;
use uuid::Uuid;

struct Downloader {
    vfs: Arc<Vfs>,
    max_downloads: usize,
    max_wait_for_match: Duration,
}

impl Downloader {
    async fn file(&self, file_id: u64) -> Result<File> {
        match self.vfs.inode_by_id(file_id).await? {
            Some(Inode::File(file)) => Ok(file),
            Some(Inode::Directory(_)) => {
                bail!("expected file but got directory");
            }
            None => {
                bail!("file not found");
            }
        }
    }

    async fn download(
        &self,
        entry: Arc<Mutex<Entry<Self>>>,
        _reservation: Reservation<Downloader>,
        file_id: u64,
        offset: u64,
    ) -> Result<Lease<Downloader>> {
        let file = self.file(file_id).await?;
        tracing::debug!(
            id = file.id(),
            name = file.name(),
            offset,
            "initiating new download"
        );
        let mut file_reader = self.vfs.read_file(&file).await?;
        file_reader.seek(SeekFrom::Start(offset)).await?;
        let mut lock = entry.lock().unwrap();
        lock.insert_tasks(vec![file_reader]);
        return Ok(lock
            .lease(&entry, offset)
            .ok_or_else(|| anyhow!("unable to get lease for inserted download task!"))?);
    }
}

impl IoBackend for Downloader {
    type Task = FileReader;
    type Key = u64;

    async fn begin(&self, key: &Self::Key) -> Result<(File, Vec<Self::Task>)> {
        let file = self.file(*key).await?;
        Ok((file, vec![]))
    }

    async fn lease(&self, entry: Arc<Mutex<Entry<Self>>>, offset: u64) -> Result<Lease<Self>> {
        let (mut watch_rx, file_id) = {
            let lock = entry.lock().unwrap();
            (lock.task_offset_change_tx.subscribe(), lock.key)
        };
        let wait_deadline = SystemTime::now() + self.max_wait_for_match;

        // try to get a download lease right now
        {
            let mut lock = entry.lock().unwrap();
            if let Some(lease) = lock.lease(&entry, offset) {
                return Ok(lease);
            }
        }

        let mut wait_for_task = false;
        if SystemTime::now() < wait_deadline {
            // check if there are any active downloads that could become potentially interesting
            let lock = entry.lock().unwrap();
            wait_for_task = lock
                .tasks
                .iter()
                .find(|t| match t {
                    Task::Active {
                        previous_offset, ..
                    } => previous_offset <= &offset,
                    _ => false,
                })
                .is_some();
        }

        if wait_for_task {
            // we won't wait longer than that
            while SystemTime::now() < wait_deadline {
                let sleep_duration = wait_deadline
                    .duration_since(SystemTime::now())
                    .unwrap_or(Duration::from_secs(0));

                tokio::select! {
                    _ = tokio::time::sleep(sleep_duration) => {},
                    _ = watch_rx.changed() => {
                        if let Some(candidate_offset) = watch_rx.borrow_and_update().deref().as_ref() {
                            if candidate_offset == &offset {
                                // it's a match
                                break;
                            }
                        }
                    }
                }
            }
        }

        let mut reservation = None;

        loop {
            // let's see if we hold a reservation
            if let Some(reservation) = match reservation.take() {
                // we do, proceed to download attempt
                Some(reservation) => Some(reservation),
                // no reservation, try to get one
                None => {
                    let mut lock = entry.lock().unwrap();
                    // first, try again if we can get a lease right away
                    if let Some(lease) = lock.lease(&entry, offset) {
                        return Ok(lease);
                    }

                    // check task capacity first, reserve if still free
                    if lock.tasks.len() < self.max_downloads {
                        // reserve a slot
                        let reservation = lock.reserve(&entry);
                        Some(reservation)
                    } else {
                        None
                    }
                }
            } {
                // managed to get a reservation
                // start a new download
                return self.download(entry, reservation, file_id, offset).await;
            }

            let mut task_to_finalize = None;
            // try to remove the oldest idle download
            {
                let mut lock = entry.lock().unwrap();
                if lock.tasks.len() >= self.max_downloads {
                    // remove the oldest idle download
                    task_to_finalize = lock.free_task();
                }
                if lock.tasks.len() < self.max_downloads {
                    // free seems to have worked
                    // get a reservation and try downloading again
                    reservation = Some(lock.reserve(&entry));
                }
            }
            if let Some(task) = task_to_finalize.take() {
                task.finalize().await?;
            }

            if reservation.is_none() {
                // everything is busy and full, try again once something changes
                watch_rx.changed().await?
            }
        }
    }
}

impl IoManager<Downloader> {
    pub(crate) fn new_downloader(
        vfs: Arc<Vfs>,
        max_idle: Duration,
        max_downloads: NonZeroUsize,
        max_wait_for_match: Duration,
    ) -> Self {
        IoManager {
            vfs: vfs.clone(),
            backend: Downloader {
                vfs,
                max_downloads: max_downloads.get(),
                max_wait_for_match,
            },
            active: Arc::new(Mutex::new(HashMap::new())),
            max_idle,
            allow_prepare_existing: true,
        }
    }
}

struct Uploader {
    vfs: Arc<Vfs>,
}

impl IoBackend for Uploader {
    type Task = FileWriter;
    type Key = (u64, String);

    async fn begin(&self, key: &Self::Key) -> Result<(File, Vec<Self::Task>)> {
        let (parent_id, name) = key;
        let parent = match self.vfs.inode_by_id(*parent_id).await? {
            Some(Inode::Directory(dir)) => dir,
            Some(Inode::File(_)) => {
                bail!("parent not a directory");
            }
            None => {
                bail!("parent does not exist");
            }
        };

        let res = self.vfs.write_file(&parent, name.to_string()).await?;
        let file = res.to_file();
        Ok((file, vec![res]))
    }

    async fn lease(&self, entry: Arc<Mutex<Entry<Self>>>, offset: u64) -> Result<Lease<Self>> {
        let mut watch_rx = { entry.lock().unwrap().task_offset_change_tx.subscribe() };
        loop {
            // try to get one right now
            {
                let mut lock = entry.lock().unwrap();
                if let Some(lease) = lock.lease(&entry, offset) {
                    return Ok(lease);
                }
            }

            // wait for an update with the exact offset
            while watch_rx
                .borrow_and_update()
                .as_ref()
                .map_or(true, |&u| u != offset)
            {
                watch_rx.changed().await?;
            }
        }
    }
}

impl IoManager<Uploader> {
    pub(crate) fn new_uploader(vfs: Arc<Vfs>, max_idle: Duration) -> Self {
        IoManager {
            vfs: vfs.clone(),
            backend: Uploader { vfs },
            active: Arc::new(Mutex::new(HashMap::new())),
            max_idle,
            allow_prepare_existing: false,
        }
    }
}

pub(crate) struct IoManager<B: IoBackend> {
    vfs: Arc<Vfs>,
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
}

impl<B: IoBackend> IoManager<B> {
    pub async fn prepare<KR: AsRef<B::Key>>(&self, key: KR) -> Result<File> {
        let key = key.as_ref();
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
                .boxed_local()
            } else {
                let (status_tx, status_rx) = watch::channel(Status::Preparing);
                let expiration = Arc::new(ArcSwap::from_pointee(
                    SystemTime::now() + Duration::from_secs(60),
                ));
                let entry = Arc::new(Mutex::new(Entry {
                    key: key.clone(),
                    tasks: vec![],
                    max_idle: self.max_idle,
                    task_offset_change_tx: watch::channel(None).0,
                    next_expiration: expiration.clone(),
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
                    .boxed_local()
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

    pub async fn file_by_key<KR: AsRef<B::Key>>(&self, key: KR) -> Result<Option<File>> {
        self.file(Either::Right(key.as_ref())).await
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

pub(crate) struct Entry<B: IoBackend> {
    key: B::Key,
    tasks: Vec<Task<B::Task>>,
    max_idle: Duration,
    task_offset_change_tx: watch::Sender<Option<u64>>,
    next_expiration: Arc<ArcSwap<SystemTime>>,
}

#[derive(PartialEq, Clone)]
pub(crate) enum Status {
    Preparing,
    Ready(File),
}

impl<B: IoBackend> Entry<B> {
    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
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
            self.next_expiration.store(Arc::new(next_expiration))
            //todo: notify
        }
    }

    fn remove_lease(&mut self, id: Uuid) {
        self.tasks = std::mem::take(&mut self.tasks)
            .into_iter()
            .filter(|t| {
                if let Task::Active { lease_id, .. } = t {
                    lease_id != &id
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();
    }

    fn return_lease(&mut self, id: Uuid, task: B::Task) {
        let offset = Some(task.offset());
        match self.tasks.iter_mut().find(|task| match task {
            Task::Active { lease_id, .. } => &id == lease_id,
            _ => false,
        }) {
            Some(t) => {
                *t = Task::Idle {
                    offset: task.offset(),
                    expiration: SystemTime::now() + self.max_idle,
                    task,
                }
            }
            None => {
                tracing::error!("unable to return lease {}", id);
            }
        }
        self.update_expiration();
        let _ = self.task_offset_change_tx.send(offset);
    }

    fn insert_tasks(&mut self, tasks: Vec<B::Task>) {
        for task in tasks {
            let offset = Some(task.offset());
            self.tasks.push(Task::Idle {
                offset: task.offset(),
                expiration: SystemTime::now() + self.max_idle,
                task,
            });
            let _ = self.task_offset_change_tx.send(offset);
        }
        self.update_expiration();
    }

    fn reserve(&mut self, entry: &Arc<Mutex<Entry<B>>>) -> Reservation<B> {
        let reservation_id = Uuid::new_v4();
        self.tasks.push(Task::Reserved { reservation_id });
        Reservation {
            entry: entry.clone(),
            reservation_id,
        }
    }

    fn return_reservation(&mut self, id: Uuid) {
        self.tasks = std::mem::take(&mut self.tasks)
            .into_iter()
            .filter(|t| {
                if let Task::Reserved { reservation_id, .. } = t {
                    reservation_id != &id
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();
    }

    /// Attempts to remove the oldest idle task
    fn free_task(&mut self) -> Option<B::Task> {
        if let Some((index, _)) = self
            .tasks
            .iter()
            .enumerate()
            .filter_map(|(index, task)| {
                if let Task::Idle { expiration, .. } = task {
                    Some((index, expiration))
                } else {
                    None
                }
            })
            .min_by_key(|&(_, expiration)| expiration)
        {
            Some(match self.tasks.swap_remove(index) {
                Task::Idle { task, .. } => task,
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

        let uuid = Uuid::new_v4();
        match std::mem::replace(
            &mut self.tasks[i],
            Task::Active {
                lease_id: uuid.clone(),
                previous_offset: offset,
            },
        ) {
            Task::Idle { offset, task, .. } => {
                if offset != wanted_offset {
                    panic!("invalid offset in task: {} != {}", wanted_offset, offset);
                }
                self.update_expiration();
                Some(Lease {
                    entry: entry.clone(),
                    lease_id: uuid,
                    backend_task: Some(task),
                })
            }
            _ => {
                unreachable!("replaced task is wrong");
            }
        }
    }
}

enum Task<BT: IoBackendTask> {
    Idle {
        offset: u64,
        expiration: SystemTime,
        task: BT,
    },
    Active {
        lease_id: Uuid,
        previous_offset: u64,
    },
    Reserved {
        reservation_id: Uuid,
    },
}

impl<BT: IoBackendTask> Task<BT> {
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

struct Reservation<B: IoBackend> {
    entry: Arc<Mutex<Entry<B>>>,
    reservation_id: Uuid,
}

impl<B: IoBackend> Drop for Reservation<B> {
    fn drop(&mut self) {
        let mut lock = self.entry.lock().unwrap();
        lock.return_reservation(self.reservation_id);
    }
}

pub(crate) struct Lease<B: IoBackend> {
    entry: Arc<Mutex<Entry<B>>>,
    lease_id: Uuid,
    backend_task: Option<B::Task>,
}

impl<B: IoBackend> Drop for Lease<B> {
    fn drop(&mut self) {
        if let Some(task) = self.backend_task.take() {
            let mut lock = self.entry.lock().unwrap();
            lock.return_lease(self.lease_id, task);
        }
    }
}

impl<B: IoBackend> Deref for Lease<B> {
    type Target = B::Task;

    fn deref(&self) -> &Self::Target {
        &self.backend_task.as_ref().unwrap()
    }
}

impl<B: IoBackend> DerefMut for Lease<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.backend_task.as_mut().unwrap()
    }
}

pub(crate) trait IoBackend: Sized {
    type Task: IoBackendTask;
    type Key: Hash + Eq + Clone;

    async fn begin(&self, key: &Self::Key) -> Result<(File, Vec<Self::Task>)>;
    async fn lease(&self, entry: Arc<Mutex<Entry<Self>>>, offset: u64) -> Result<Lease<Self>>;
}

pub(crate) trait IoBackendTask {
    fn offset(&self) -> u64;
    async fn finalize(self) -> Result<File>;
}

impl IoBackendTask for FileReader {
    fn offset(&self) -> u64 {
        self.offset()
    }

    async fn finalize(self) -> Result<File> {
        Ok(self.file().clone())
    }
}

impl IoBackendTask for FileWriter {
    fn offset(&self) -> u64 {
        self.bytes_written()
    }

    async fn finalize(self) -> Result<File> {
        self.finalize().await
    }
}
