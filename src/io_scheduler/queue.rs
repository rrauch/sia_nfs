use crate::io_scheduler::BackendTask;
use crate::vfs::File;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use itertools::{Either, Itertools};
use std::collections::HashMap;
use std::num::NonZeroUsize;
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
    Expired(u64, SystemTime),
}

impl Activity {
    pub fn timestamp(&self) -> SystemTime {
        match &self {
            Activity::Waiting(_, ts) => ts.clone(),
            Activity::Idle(_, ts) => ts.clone(),
            Activity::Active(_, ts) => ts.clone(),
            Activity::Reserved(_, ts) => ts.clone(),
            Activity::Expired(_, ts) => ts.clone(),
        }
    }

    pub fn offset(&self) -> u64 {
        match &self {
            Activity::Waiting(offset, _) => offset.clone(),
            Activity::Idle(offset, _) => offset.clone(),
            Activity::Active(offset, _) => offset.clone(),
            Activity::Reserved(offset, _) => offset.clone(),
            Activity::Expired(offset, _) => offset.clone(),
        }
    }
}

pub(super) struct Queue<BT: BackendTask> {
    file_tx: watch::Sender<File>,
    file_rx: watch::Receiver<File>,
    last_activity: SystemTime,
    active: usize,
    max_active: usize, // this includes reserved
    max_idle: Duration,
    queue: HashMap<usize, QEntry<BT>>,
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
        let (expiration_tx, _) = watch::channel(initial_expiration);
        let (file_tx, file_rx) = watch::channel(file);
        let now = SystemTime::now();
        let mut id_counter = 0;
        let mut active = 0;
        Self {
            file_tx,
            file_rx,
            last_activity: now,
            max_active: max_active.get(),
            max_idle,
            queue: initial_tasks
                .into_iter()
                .filter_map(|t| {
                    if t.can_reuse() {
                        let id = id_counter;
                        id_counter += 1;
                        active += 1;
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
            id_counter,
            active,
            activity_tx,
            expiration_tx,
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active > 0
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn activity(&self) -> broadcast::Receiver<Activity> {
        self.activity_tx.subscribe()
    }

    pub fn expiration(&self) -> watch::Receiver<SystemTime> {
        self.expiration_tx.subscribe()
    }

    pub fn file(&self) -> watch::Receiver<File> {
        self.file_tx.subscribe()
    }

    pub fn last_activity(&self) -> SystemTime {
        // if active return the current time
        if self.active > 0 {
            return SystemTime::now();
        }
        self.last_activity
    }

    fn update_expiration(&self) -> Option<SystemTime> {
        let mut expiration = None;
        self.queue.values().for_each(|qe| {
            if let Op::Idle(_) = qe.op {
                let op_exp = qe.since + self.max_idle;
                if let Some(lowest_exp) = expiration {
                    if op_exp < lowest_exp {
                        expiration = Some(op_exp);
                    }
                } else {
                    expiration = Some(op_exp);
                }
            }
        });
        expiration
    }

    pub fn remove_expired_idle(&mut self) -> Vec<BT> {
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
                        .send(Activity::Expired(task.offset(), SystemTime::now()));
                    task
                }
                _ => unreachable!(),
            })
            .collect_vec();

        self.active -= tasks.len();
        self.update_expiration();
        tasks
    }

    /// Returns whether a possible candidate for the waiter is in the queue or not.
    /// A possible candidate is any active or idle Op currently in the queue that could become
    /// the one the waiter wants
    pub fn contains_candidate(&self, wait_handle: &WaitHandle) -> bool {
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

    /// This function tries to remove the least "valuable" idle Op to
    /// make space for a new reservation
    /// The resulting `FreedHandle` needs to be finalized before it can be used
    /// as a ReserveHandle
    pub fn try_free(&mut self, wait_handle: WaitHandle) -> Either<FreedHandle<BT>, WaitHandle> {
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
                    .send(Activity::Expired(task.offset(), SystemTime::now()));
                task
            }
            _ => unreachable!(),
        };
        self.return_handle(wait_handle);
        self.update_expiration();
        Either::Left(FreedHandle {
            task,
            handle: ReserveHandle {
                id,
                since: now,
                offset,
            },
        })
    }

    pub fn wait(&mut self, offset: u64) -> WaitHandle {
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
        let _ = self.activity_tx.send(Activity::Waiting(offset, now));
        WaitHandle {
            id,
            offset,
            since: now,
        }
    }

    pub fn resume(&mut self, wait_handle: WaitHandle) -> Either<ActiveHandle<BT>, WaitHandle> {
        // find an idle op at the requested offset
        let (qe, id) = match self
            .queue
            .iter()
            .find(|(k, v)| {
                v.offset == wait_handle.offset
                    && match v.op {
                        Op::Idle(_) => true,
                        _ => false,
                    }
            })
            .map(|(k, v)| k.clone())
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
        self.return_handle(wait_handle);
        self.update_expiration();
        return Either::Left(ActiveHandle {
            id,
            initial_offset: offset,
            since: now,
            task,
        });
    }

    pub fn reserve(&mut self, wait_handle: WaitHandle) -> Either<ReserveHandle, WaitHandle> {
        if self.active >= self.max_active {
            return Either::Right(wait_handle);
        }

        let entry = self
            .queue
            .get_mut(&wait_handle.id)
            .expect("unable to find matching waiting op for wait_handle");

        let now = SystemTime::now();
        entry.since = now;
        entry.op = Op::Reserved;

        self.active += 1; // we count reserved ops as active
        let _ = self
            .activity_tx
            .send(Activity::Reserved(wait_handle.offset, now));
        Either::Left(ReserveHandle {
            id: wait_handle.id,
            since: now,
            offset: wait_handle.offset,
        })
    }

    pub fn redeem(&mut self, reserve_handle: ReserveHandle, task: BT) -> ActiveHandle<BT> {
        let entry = self
            .queue
            .get_mut(&reserve_handle.id)
            .expect("unable to find matching reserve op for reserve_handle");

        let now = SystemTime::now();
        entry.since = now;
        entry.offset = task.offset();
        entry.op = Op::Active;
        let _ = self.activity_tx.send(Activity::Active(task.offset(), now));
        ActiveHandle {
            id: reserve_handle.id,
            since: now,
            initial_offset: entry.offset,
            task,
        }
    }

    pub fn return_handle<T: Into<Handle<BT>>>(&mut self, handle: T) {
        let id = match handle.into() {
            Handle::Wait(handle) => Some(handle.id),
            Handle::Reserve(handle) => {
                if self.queue.contains_key(&handle.id) {
                    self.active -= 1;
                }
                Some(handle.id)
            }
            Handle::Active(handle) => {
                let now = SystemTime::now();
                self.last_activity = now;
                if handle.task.can_reuse() {
                    let _ = self.file_tx.send(handle.task.to_file());
                    if let Some(qe) = self.queue.get_mut(&handle.id) {
                        tracing::trace!(
                            offset = handle.task.offset(),
                            "returning reusable task to queue"
                        );
                        qe.since = now;
                        qe.offset = handle.task.offset();
                        qe.op = Op::Idle(handle.task);
                        let _ = self.activity_tx.send(Activity::Idle(qe.offset, now));
                    }
                    // active counter is not reduced because idle is still counted as active
                    None
                } else {
                    tracing::debug!(
                        offset = handle.task.offset(),
                        "task not reusable, discarding"
                    );
                    // task can not be reused and has to be discarded
                    if self.queue.contains_key(&handle.id) {
                        self.active -= 1;
                    }
                    Some(handle.id)
                }
            }
        };
        if let Some(id) = id {
            if self.queue.remove(&id).is_none() {
                tracing::warn!("no queue entry found for id {} while returning handle", id);
            }
        }
        self.update_expiration();
    }
}

enum Handle<BT: BackendTask> {
    Wait(WaitHandle),
    Reserve(ReserveHandle),
    Active(ActiveHandle<BT>),
}

impl<BT: BackendTask> From<WaitHandle> for Handle<BT> {
    fn from(h: WaitHandle) -> Self {
        Handle::Wait(h)
    }
}

impl<BT: BackendTask> From<ReserveHandle> for Handle<BT> {
    fn from(h: ReserveHandle) -> Self {
        Handle::Reserve(h)
    }
}

impl<BT: BackendTask> From<ActiveHandle<BT>> for Handle<BT> {
    fn from(h: ActiveHandle<BT>) -> Self {
        Handle::Active(h)
    }
}

pub struct WaitHandle {
    id: usize,
    offset: u64,
    since: SystemTime,
}

pub struct ReserveHandle {
    id: usize,
    offset: u64,
    since: SystemTime,
}

pub(super) struct ActiveHandle<BT: BackendTask> {
    id: usize,
    initial_offset: u64,
    since: SystemTime,
    task: BT,
}

impl<BT: BackendTask> AsRef<BT> for ActiveHandle<BT> {
    fn as_ref(&self) -> &BT {
        &self.task
    }
}

impl<BT: BackendTask> AsMut<BT> for ActiveHandle<BT> {
    fn as_mut(&mut self) -> &mut BT {
        &mut self.task
    }
}

pub struct FreedHandle<BT: BackendTask> {
    task: BT,
    handle: ReserveHandle,
}

impl<BT: BackendTask> FreedHandle<BT> {
    pub async fn finalize(self) -> (anyhow::Result<()>, ReserveHandle) {
        let res = self.task.finalize().await;
        (res, self.handle)
    }
}
