use crate::io_scheduler::BackendTask;
use crate::vfs::inode::File;
use itertools::{Either, Itertools};
use parking_lot::{Mutex, MutexGuard};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{broadcast, watch};

struct QEntry<BT: BackendTask> {
    offset: u64,
    since: SystemTime,
    op: Op<BT>,
}

enum Op<BT: BackendTask> {
    Waiting,
    Idle(BT),
    Active,
    Reserved,
}

#[derive(Clone)]
pub(super) enum Activity {
    Waiting(u64, SystemTime),
    Idle(u64, SystemTime),
    Active(u64, SystemTime),
    Reserved(u64, SystemTime),
    Removed(u64, SystemTime),
}

impl Activity {
    pub fn timestamp(&self) -> SystemTime {
        match &self {
            Activity::Waiting(_, ts) => ts.clone(),
            Activity::Idle(_, ts) => ts.clone(),
            Activity::Active(_, ts) => ts.clone(),
            Activity::Reserved(_, ts) => ts.clone(),
            Activity::Removed(_, ts) => ts.clone(),
        }
    }

    pub fn offset(&self) -> u64 {
        match &self {
            Activity::Waiting(offset, _) => offset.clone(),
            Activity::Idle(offset, _) => offset.clone(),
            Activity::Active(offset, _) => offset.clone(),
            Activity::Reserved(offset, _) => offset.clone(),
            Activity::Removed(offset, _) => offset.clone(),
        }
    }
}

pub(super) struct Queue<BT: BackendTask> {
    file_rx: watch::Receiver<File>,
    last_activity_rx: watch::Receiver<SystemTime>,
    active: Arc<AtomicUsize>,
    queue_len: Arc<AtomicUsize>,
    expiration_rx: watch::Receiver<SystemTime>,
    activity_tx: broadcast::Sender<Activity>,
    shared: Arc<Mutex<Shared<BT>>>,
}

struct Shared<BT: BackendTask> {
    file_tx: watch::Sender<File>,
    last_activity_tx: watch::Sender<SystemTime>,
    active: Arc<AtomicUsize>,
    max_active: usize, // this includes reserved
    max_idle: Duration,
    queue: HashMap<usize, QEntry<BT>>,
    queue_len: Arc<AtomicUsize>,
    id_counter: usize,
    activity_tx: broadcast::Sender<Activity>,
    expiration_tx: watch::Sender<SystemTime>,
}

impl<BT: BackendTask> Queue<BT> {
    pub(super) fn new(
        max_active: NonZeroUsize,
        max_idle: Duration,
        initial_expiration: SystemTime,
        initial_tasks: Vec<BT>,
        file: File,
    ) -> Self {
        let (activity_tx, _) = broadcast::channel(30);
        let (expiration_tx, expiration_rx) = watch::channel(initial_expiration);
        let (file_tx, file_rx) = watch::channel(file);
        let now = SystemTime::now();
        let (last_activity_tx, last_activity_rx) = watch::channel(now);
        let mut id_counter = 0;
        let active = Arc::new(AtomicUsize::new(0));
        let queue_len = Arc::new(AtomicUsize::new(0));
        let shared = Arc::new(Mutex::new(Shared {
            file_tx,
            last_activity_tx,
            active: active.clone(),
            max_active: max_active.get(),
            max_idle,
            queue: initial_tasks
                .into_iter()
                .filter_map(|t| {
                    if t.can_reuse() {
                        let id = id_counter;
                        id_counter += 1;
                        active.fetch_add(1, Ordering::SeqCst);
                        Some((
                            id,
                            QEntry {
                                since: now,
                                offset: t.offset(),
                                op: Op::Idle(t),
                            },
                        ))
                    } else {
                        None
                    }
                })
                .collect::<HashMap<_, _>>(),
            queue_len: queue_len.clone(),
            id_counter,
            activity_tx: activity_tx.clone(),
            expiration_tx,
        }));
        Self {
            file_rx,
            last_activity_rx,
            active,
            queue_len,
            expiration_rx,
            activity_tx,
            shared,
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst) > 0
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.queue_len.load(Ordering::SeqCst)
    }

    pub fn activity(&self) -> broadcast::Receiver<Activity> {
        self.activity_tx.subscribe()
    }

    pub fn expiration(&self) -> watch::Receiver<SystemTime> {
        self.expiration_rx.clone()
    }

    pub fn file(&self) -> watch::Receiver<File> {
        self.file_rx.clone()
    }

    pub fn last_activity(&self) -> SystemTime {
        // if active return the current time
        if self.is_active() {
            return SystemTime::now();
        }
        *self.last_activity_rx.borrow()
    }

    pub fn lock(&self) -> QueueGuard<BT> {
        QueueGuard {
            shared: self.shared.clone(),
            inner: self.shared.lock(),
        }
    }
}

pub(super) struct QueueGuard<'a, BT: BackendTask> {
    shared: Arc<Mutex<Shared<BT>>>,
    inner: MutexGuard<'a, Shared<BT>>,
}
impl<BT: BackendTask> QueueGuard<'_, BT> {
    pub fn wait(&mut self, offset: u64) -> WaitHandle<BT> {
        self.inner.wait(offset, &self.shared)
    }

    pub fn resume(
        &mut self,
        wait_handle: WaitHandle<BT>,
    ) -> Either<ActiveHandle<BT>, WaitHandle<BT>> {
        self.inner.resume(wait_handle)
    }

    /// Returns whether a possible candidate for the waiter is in the queue or not.
    /// A possible candidate is any active or idle Op currently in the queue that could become
    /// the one the waiter wants
    pub fn contains_candidate(&self, wait_handle: &WaitHandle<BT>) -> bool {
        self.inner.contains_candidate(wait_handle)
    }

    pub fn redeem(&mut self, reserve_handle: ReserveHandle<BT>, task: BT) -> ActiveHandle<BT> {
        self.inner.redeem(reserve_handle, task)
    }

    pub fn reserve(
        &mut self,
        wait_handle: WaitHandle<BT>,
    ) -> Either<ReserveHandle<BT>, WaitHandle<BT>> {
        self.inner.reserve(wait_handle)
    }

    /// This function tries to remove the least "valuable" idle Op to
    /// make space for a new reservation
    /// The resulting `FreedHandle` needs to be finalized before it can be used
    /// as a ReserveHandle
    pub fn try_free(
        &mut self,
        wait_handle: WaitHandle<BT>,
    ) -> Either<FreedHandle<BT>, WaitHandle<BT>> {
        self.inner.try_free(wait_handle)
    }

    pub fn remove_expired_idle(&mut self) -> Vec<BT> {
        self.inner.remove_expired_idle()
    }
}
impl<BT: BackendTask> Shared<BT> {
    fn update_stats(&self) {
        let mut active = 0;
        let mut expiration = None;
        self.queue.values().for_each(|qe| match qe.op {
            Op::Idle(_) => {
                let op_exp = qe.since + self.max_idle;
                if let Some(lowest_exp) = expiration {
                    if op_exp < lowest_exp {
                        expiration = Some(op_exp);
                    }
                } else {
                    expiration = Some(op_exp);
                }
            }
            Op::Active | Op::Reserved => {
                active += 1;
            }
            _ => {}
        });
        self.active.store(active, Ordering::SeqCst);
        self.queue_len.store(self.queue.len(), Ordering::SeqCst);
        if let Some(expiration) = expiration {
            if &expiration != self.expiration_tx.borrow().deref() {
                let _ = self.expiration_tx.send(expiration);
            }
        }
    }

    fn remove_expired_idle(&mut self) -> Vec<BT> {
        let deadline = SystemTime::now() - self.max_idle;
        // once `extract_if` has been stabilized this can be used instead of the two-step manual way below
        // see https://github.com/rust-lang/rust/issues/59618
        let ids = self
            .queue
            .iter()
            .filter_map(|(id, qe)| {
                if qe.since <= deadline
                    && match qe.op {
                        Op::Idle(_) => true,
                        _ => false,
                    }
                {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect_vec();

        let tasks = ids
            .iter()
            .map(|id| self.queue.remove(id))
            .flatten()
            .map(|entry| match entry.op {
                Op::Idle(task) => {
                    let _ = self
                        .activity_tx
                        .send(Activity::Removed(task.offset(), SystemTime::now()));
                    task
                }
                _ => unreachable!(),
            })
            .collect_vec();

        self.update_stats();
        tasks
    }

    fn contains_candidate(&self, wait_handle: &WaitHandle<BT>) -> bool {
        self.queue
            .iter()
            .find(|(id, qe)| {
                id != &&wait_handle.id
                    && qe.offset <= wait_handle.offset
                    && match qe.op {
                        Op::Active | Op::Reserved | Op::Idle(_) => true,
                        _ => false,
                    }
            })
            .is_some()
    }

    fn try_free(
        &mut self,
        mut wait_handle: WaitHandle<BT>,
    ) -> Either<FreedHandle<BT>, WaitHandle<BT>> {
        // this gets the oldest (idle the longest) idle entry
        let (id, entry) = match self
            .queue
            .iter()
            .filter_map(|(id, qe)| match qe.op {
                Op::Idle(_) => Some((qe.offset, qe.since, *id)),
                _ => None,
            })
            .sorted_unstable_by(|a, b| {
                match a.1.cmp(&b.1) {
                    // Sort by SystemTime, oldest first
                    std::cmp::Ordering::Equal => b.0.cmp(&a.0), // Then sort by offset, highest first
                    other => other,
                }
            })
            .next()
            .map(|(_, _, id)| (id, self.queue.get_mut(&id)))
        {
            Some((id, Some(qe))) => (id, qe),
            _ => {
                // nothing idle right now
                return Either::Right(wait_handle);
            }
        };

        let now = SystemTime::now();
        let offset = wait_handle.offset;
        entry.since = now;
        entry.offset = offset;
        let task = match std::mem::replace(&mut entry.op, Op::Reserved) {
            Op::Idle(task) => {
                let _ = self
                    .activity_tx
                    .send(Activity::Removed(task.offset(), SystemTime::now()));
                task
            }
            _ => unreachable!(),
        };
        let inner = wait_handle.shared.clone();
        wait_handle.return_handle(self);
        self.update_stats();
        Either::Left(FreedHandle {
            task,
            handle: ReserveHandle {
                id,
                offset,
                shared: inner,
                armed: true,
            },
        })
    }

    fn wait(&mut self, offset: u64, inner: &Arc<Mutex<Shared<BT>>>) -> WaitHandle<BT> {
        let now = SystemTime::now();
        let id = self.id_counter;
        self.id_counter += 1;

        self.queue.insert(
            id,
            QEntry {
                offset,
                since: now,
                op: Op::Waiting,
            },
        );
        self.update_stats();
        let _ = self.activity_tx.send(Activity::Waiting(offset, now));
        WaitHandle {
            id,
            offset,
            shared: inner.clone(),
            armed: true,
        }
    }

    fn resume(
        &mut self,
        mut wait_handle: WaitHandle<BT>,
    ) -> Either<ActiveHandle<BT>, WaitHandle<BT>> {
        // find an idle op at the requested offset
        let (qe, id) = match self
            .queue
            .iter()
            .find(|(_, v)| {
                v.offset == wait_handle.offset
                    && match v.op {
                        Op::Idle(_) => true,
                        _ => false,
                    }
            })
            .map(|(k, _)| k.clone())
            .map(|id| self.queue.get_mut(&id).map(|e| (e, id)))
            .flatten()
        {
            Some((qe, id)) => (qe, id),
            None => {
                return Either::Right(wait_handle);
            }
        };

        let now = SystemTime::now();
        qe.since = now;
        let task = match std::mem::replace(&mut qe.op, Op::Active) {
            Op::Idle(task) => {
                let _ = self.activity_tx.send(Activity::Active(task.offset(), now));
                task
            }
            _ => unreachable!(),
        };
        let offset = task.offset();
        qe.offset = offset;
        let inner = wait_handle.shared.clone();
        wait_handle.return_handle(self); // clean up now
        self.update_stats();
        return Either::Left(ActiveHandle {
            id,
            initial_offset: offset,
            task: Some(task),
            shared: inner,
        });
    }

    fn reserve(
        &mut self,
        mut wait_handle: WaitHandle<BT>,
    ) -> Either<ReserveHandle<BT>, WaitHandle<BT>> {
        if self.active.load(Ordering::SeqCst) >= self.max_active {
            return Either::Right(wait_handle);
        }

        let entry = self
            .queue
            .get_mut(&wait_handle.id)
            .expect("unable to find matching waiting op for wait_handle");

        let now = SystemTime::now();
        entry.since = now;
        entry.op = Op::Reserved;

        wait_handle.disarm();

        self.update_stats();

        let _ = self
            .activity_tx
            .send(Activity::Reserved(wait_handle.offset, now));

        Either::Left(ReserveHandle {
            id: wait_handle.id,
            offset: wait_handle.offset,
            shared: wait_handle.shared.clone(),
            armed: true,
        })
    }

    fn redeem(&mut self, mut reserve_handle: ReserveHandle<BT>, task: BT) -> ActiveHandle<BT> {
        let entry = self
            .queue
            .get_mut(&reserve_handle.id)
            .expect("unable to find matching reserve op for reserve_handle");

        let now = SystemTime::now();
        entry.since = now;
        entry.offset = task.offset();
        entry.op = Op::Active;

        reserve_handle.disarm();

        let _ = self.activity_tx.send(Activity::Active(task.offset(), now));
        ActiveHandle {
            id: reserve_handle.id,
            initial_offset: entry.offset,
            task: Some(task),
            shared: reserve_handle.shared.clone(),
        }
    }
}

pub struct WaitHandle<BT: BackendTask> {
    id: usize,
    offset: u64,
    shared: Arc<Mutex<Shared<BT>>>,
    armed: bool,
}

impl<BT: BackendTask> WaitHandle<BT> {
    fn return_handle(&mut self, shared: &mut Shared<BT>) {
        self.armed = false;
        if shared.queue.remove(&self.id).is_none() {
            let now = SystemTime::now();
            let _ = shared.last_activity_tx.send(now);
            let _ = shared.activity_tx.send(Activity::Removed(self.offset, now));

            tracing::warn!(
                "no queue entry found for id {} while returning wait handle",
                self.id
            );
        }
        shared.update_stats();
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<BT: BackendTask> Drop for WaitHandle<BT> {
    fn drop(&mut self) {
        if self.armed {
            let shared = self.shared.clone();
            let mut shared = shared.lock();
            self.return_handle(&mut shared);
        }
    }
}

pub struct ReserveHandle<BT: BackendTask> {
    id: usize,
    offset: u64,
    shared: Arc<Mutex<Shared<BT>>>,
    armed: bool,
}

impl<BT: BackendTask> ReserveHandle<BT> {
    fn return_handle(&mut self, shared: &mut Shared<BT>) {
        self.armed = false;
        if shared.queue.remove(&self.id).is_some() {
            // decrease active count
            shared.active.fetch_sub(1, Ordering::SeqCst);
            let now = SystemTime::now();
            let _ = shared.last_activity_tx.send(now);
            let _ = shared.activity_tx.send(Activity::Removed(self.offset, now));
        } else {
            tracing::warn!(
                "no queue entry found for id {} while returning reserved handle",
                self.id
            );
        }
        shared.update_stats();
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<BT: BackendTask> Drop for ReserveHandle<BT> {
    fn drop(&mut self) {
        if self.armed {
            let shared = self.shared.clone();
            let mut shared = shared.lock();
            self.return_handle(&mut shared);
        }
    }
}

pub(crate) struct ActiveHandle<BT: BackendTask> {
    id: usize,
    initial_offset: u64,
    task: Option<BT>,
    shared: Arc<Mutex<Shared<BT>>>,
}

impl<BT: BackendTask> Drop for ActiveHandle<BT> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            let now = SystemTime::now();
            let shared = self.shared.clone();
            let mut shared = shared.lock();
            let _ = shared.last_activity_tx.send(now);
            let activity_tx = shared.activity_tx.clone();
            if task.can_reuse() {
                let _ = shared.file_tx.send(task.to_file());
                if let Some(qe) = shared.queue.get_mut(&self.id) {
                    tracing::trace!(offset = task.offset(), "returning reusable task to queue");
                    qe.since = now;
                    qe.offset = task.offset();
                    qe.op = Op::Idle(task);
                    let _ = activity_tx.send(Activity::Idle(qe.offset, now));
                }
                // active counter is not reduced because idle is still counted as active
            } else {
                tracing::debug!(offset = task.offset(), "task not reusable, discarding");
                // task can not be reused and has to be discarded
                if shared.queue.remove(&self.id).is_some() {
                    // decrease active count
                    shared.active.fetch_sub(1, Ordering::SeqCst);
                    let _ = activity_tx.send(Activity::Removed(self.initial_offset, now));
                } else {
                    tracing::warn!(
                        "no queue entry found for id {} while discarding active handle",
                        self.id
                    );
                }
            }
            shared.update_stats();
        }
    }
}

impl<BT: BackendTask> AsRef<BT> for ActiveHandle<BT> {
    fn as_ref(&self) -> &BT {
        self.task.as_ref().unwrap()
    }
}

impl<BT: BackendTask> AsMut<BT> for ActiveHandle<BT> {
    fn as_mut(&mut self) -> &mut BT {
        self.task.as_mut().unwrap()
    }
}

pub struct FreedHandle<BT: BackendTask> {
    task: BT,
    handle: ReserveHandle<BT>,
}

impl<BT: BackendTask> FreedHandle<BT> {
    pub async fn finalize(self) -> anyhow::Result<ReserveHandle<BT>> {
        self.task.finalize().await?;
        Ok(self.handle)
    }
}
