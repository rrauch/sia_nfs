use super::ResourceManager;
use crate::io_scheduler::queue::ctrl::QueueCtrl;
use crate::io_scheduler::resource_manager::{Action, Context, Resource};
use anyhow::{bail, Result};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::{CancellationToken, DropGuard};

type Response<R> = oneshot::Sender<Result<ActiveHandle<R>>>;

struct QEntry<R: Resource + 'static> {
    offset: u64,
    since: SystemTime,
    op: Op<R>,
}

impl<R: Resource + 'static> QEntry<R> {
    /// Resets the entry state from `Reserved` back to `Waiting`
    fn reset_reserved(&mut self) -> bool {
        let op = std::mem::replace(&mut self.op, Op::Invalid);
        match op {
            Op::Reserved(resp, since) => {
                self.op = Op::Waiting(resp);
                self.since = since;
                true
            }
            _ => {
                self.op = op;
                false
            }
        }
    }

    /// Moves the entry state from `Reserved` to `Active` and sends the resource to the listener.
    /// Will return the resource on failure
    fn active(
        &mut self,
        resource: R,
        id: usize,
        return_tx: &mpsc::Sender<(usize, R)>,
    ) -> std::result::Result<(), R> {
        if resource.offset() != self.offset {
            tracing::error!(
                "cannot activate entry, invalid offset: {} != {}",
                self.offset,
                resource.offset()
            );
            return Err(resource);
        }
        let op = std::mem::replace(&mut self.op, Op::Active);
        let resp = match op {
            Op::Reserved(resp, _) => resp,
            Op::Waiting(resp) => resp,
            _ => {
                tracing::error!("cannot activate entry, state is neither Reserved nor Waiting");
                self.op = op;
                return Err(resource);
            }
        };
        self.since = SystemTime::now();
        if let Err(resource) = resp
            .send(Ok(ActiveHandle {
                id,
                resource: Some(resource),
                return_tx: return_tx.clone(),
            }))
            .map_err(|e| e.ok().unwrap().resource.take().unwrap())
        {
            tracing::warn!("receiver closed already");
            self.op = Op::Invalid;
            return Err(resource);
        }
        tracing::trace!(offset = self.offset, "reusing existing resource");
        Ok(())
    }

    fn is_valid(&self) -> bool {
        match &self.op {
            Op::Invalid => false,
            Op::Waiting(resp) | Op::Reserved(resp, _) => !resp.is_closed(),
            Op::Idle(resource) => resource.can_reuse(),
            _ => true,
        }
    }

    fn take_resource(&mut self) -> Option<R> {
        let op = std::mem::replace(&mut self.op, Op::Invalid);
        if let Op::Idle(resource) = op {
            Some(resource)
        } else {
            self.op = op;
            None
        }
    }
}

enum Op<R: Resource + 'static> {
    Waiting(Response<R>),
    Idle(R),
    Active,
    Reserved(Response<R>, SystemTime),
    Invalid,
}

pub(super) struct Queue<RM: ResourceManager>
where
    <RM as ResourceManager>::Resource: 'static,
{
    resource_tx: mpsc::Sender<(u64, Response<RM::Resource>)>,
    runner: JoinHandle<()>,
    ct: CancellationToken,
    _drop_guard: DropGuard,
}

impl<RM: ResourceManager + Send + Sync + 'static> Queue<RM> {
    pub(super) fn new(
        resource_manager: Arc<RM>,
        resource_data: RM::ResourceData,
        initial_resources: Vec<RM::Resource>,
        max_idle: Duration,
        max_resource_idle: Duration,
        max_prep_errors: usize,
        term_fn: Option<impl FnOnce() -> BoxFuture<'static, ()> + Send + 'static>,
    ) -> Self {
        let (resource_tx, resource_rx) = mpsc::channel(10);
        let (return_tx, return_rx) = mpsc::channel(10);
        let ct = CancellationToken::new();
        let runner = {
            let queue = QueueInner::new(
                initial_resources,
                ct.clone(),
                return_tx,
                max_idle,
                max_resource_idle,
                max_prep_errors,
            );
            let term_fn = term_fn.map(|f| {
                Box::new(f) as Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send + 'static>
            });
            tokio::spawn(async move {
                Self::run(
                    queue,
                    resource_manager,
                    resource_data,
                    resource_rx,
                    return_rx,
                    term_fn,
                )
                .await;
            })
        };

        Self {
            resource_tx,
            runner,
            ct: ct.clone(),
            _drop_guard: ct.drop_guard(),
        }
    }

    pub(super) async fn access(&self, offset: u64) -> Result<ActiveHandle<RM::Resource>>
    where
        <RM as ResourceManager>::Resource: 'static,
    {
        let (res_tx, res_rx) = oneshot::channel();
        self.resource_tx.send((offset, res_tx)).await?;
        res_rx.await?
    }

    pub(super) async fn shutdown(self) {
        self.ct.cancel();
        let _ = self.runner.await;
    }

    async fn run(
        mut queue: QueueInner<RM>,
        rm: Arc<RM>,
        mut resource_data: RM::ResourceData,
        mut resource_rx: mpsc::Receiver<(u64, Response<RM::Resource>)>,
        mut return_rx: mpsc::Receiver<(usize, RM::Resource)>,
        mut term_fn: Option<Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>>,
    ) {
        let mut prepare_resources: JoinSet<(usize, Result<<RM as ResourceManager>::Resource>)> =
            JoinSet::new();
        // add a dummy entry that never resolves
        prepare_resources.spawn(async move {
            // wait forever
            future::pending::<()>().await;
            unreachable!()
        });
        let mut finalize_resources: JoinSet<()> = JoinSet::new();

        let mut ctx = Context {
            started: queue.started,
            last_activity: SystemTime::now(),
            previous_call: None,
            iteration: 0,
        };

        loop {
            // cleanup
            for resource in queue.cleanup() {
                finalize_resources.spawn(async move {
                    let _ = resource.finalize().await;
                });
            }

            // match waiting & idle first
            if queue.match_waiting() > 0 {
                ctx.last_activity = SystemTime::now();
            }

            let mut next_processing = SystemTime::now() + Duration::from_secs(86400);

            if queue.wait_count() > 0 {
                // work needs to be done here
                let mut break_loop = false;
                let mut ctrl = QueueCtrl::new(&mut queue);
                let call_start = SystemTime::now();
                match rm.process(&mut ctrl, &mut resource_data, &ctx) {
                    Ok(Action::Sleep(duration)) => {
                        let duration = max(duration, Duration::from_millis(1)); // make sure sleep duration is at least 1 ms
                        tracing::trace!(
                            duration_ms = duration.as_millis(),
                            "next scheduled resource manager call"
                        );
                        next_processing = SystemTime::now() + duration;
                    }
                    Ok(Action::Again) => {
                        tracing::trace!("resource manager will be called again");
                        next_processing = SystemTime::now();
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "resource_manager returned error, shutting down queue");
                        break_loop = true;
                    }
                }
                ctx.iteration += 1;
                ctx.previous_call = Some(call_start);

                if !ctrl.futures.is_empty() || !ctrl.finalize_resources.is_empty() {
                    ctx.last_activity = SystemTime::now();
                }

                for (id, fut) in ctrl.futures {
                    prepare_resources.spawn(async move { (id, fut.await) });
                }
                for resource in ctrl.finalize_resources {
                    finalize_resources.spawn(async move {
                        let _ = resource.finalize().await;
                    });
                }

                if break_loop {
                    break;
                }
            }

            let now = SystemTime::now();

            let next_processing_in = next_processing.duration_since(now).unwrap_or_default();

            let resource_inactivity_timeout_in = queue
                .next_idle_resource_expiration()
                .map_or(Duration::from_secs(86400 * 365), |exp| {
                    exp.duration_since(now).unwrap_or_default()
                });

            let next_run_in = min(resource_inactivity_timeout_in, next_processing_in);

            let inactivity_timeout_in = if queue.is_idle() {
                (ctx.last_activity + queue.max_idle)
                    .duration_since(now)
                    .unwrap_or_default()
            } else {
                Duration::from_secs(86400 * 365)
            };

            tokio::select! {
                Some((offset, resp)) = resource_rx.recv() => {
                    queue.wait(offset, resp);
                    ctx.last_activity = SystemTime::now();
                },
                Some((id, resource)) = return_rx.recv() => {
                    queue.return_resource(id, resource);
                    ctx.last_activity = SystemTime::now();
                }
                Some(res) = prepare_resources.join_next() => {
                    match res {
                        Ok((id, Ok(resource))) => {
                            if let Err(err) = queue.redeem(id, Some(resource)).await {
                                tracing::error!(error = %err, "unable to redeem resource");
                            }
                        }
                        Ok((id, Err(err))) => {
                            queue.prep_errors_left = queue.prep_errors_left.saturating_sub(1);
                            if queue.prep_errors_left == 0 {
                                tracing::error!("too many preparation errors, shutting down queue");
                                break;
                            }
                            let _ = queue.redeem(id, None);
                            tracing::error!(error = %err, "preparing resource failed");
                        }
                        Err(err) => {
                            tracing::error!(error = %err, "unable to prepare resource, shutting down queue");
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(next_run_in) => {
                    // run the loop now
                }
                _ = tokio::time::sleep(inactivity_timeout_in) => {
                    // inactivity timeout reached
                    // exit loop
                    break;
                }
                _ = queue.ct.cancelled() => {
                    // shutdown requested
                    // exit loop
                    break;
                }
            }
        }
        // make sure the channels are closed first
        drop(resource_rx);
        drop(return_rx);

        // finalize idle resources
        for (_, v) in queue.map.drain() {
            if let Op::Idle(resource) = v.op {
                finalize_resources.spawn(async move {
                    let _ = resource.finalize().await;
                });
            }
        }

        // execute termination function if set
        if let Some(term_fn) = term_fn.take() {
            term_fn().await;
        }

        // cleanup
        prepare_resources.abort_all();

        // wait for all tasks to end
        while let Some(_) = prepare_resources.join_next().await {}
        while let Some(_) = finalize_resources.join_next().await {}
    }
}

struct QueueInner<RM: ResourceManager + Send + Sync + 'static>
where
    <RM as ResourceManager>::Resource: 'static,
{
    started: SystemTime,
    ct: CancellationToken,
    return_tx: mpsc::Sender<(usize, RM::Resource)>,
    max_idle: Duration,
    max_resource_idle: Duration,
    id_counter: usize,
    map: HashMap<usize, QEntry<RM::Resource>>,
    prep_errors_left: usize,
}

impl<RM: ResourceManager + Send + Sync + 'static> QueueInner<RM> {
    fn new(
        initial_resources: Vec<RM::Resource>,
        ct: CancellationToken,
        return_tx: mpsc::Sender<(usize, RM::Resource)>,
        max_idle: Duration,
        max_resource_idle: Duration,
        max_prep_errors: usize,
    ) -> Self {
        let mut id_counter = 0;
        let mut map = HashMap::new();
        for resource in initial_resources {
            let id = id_counter;
            id_counter += 1;
            map.insert(
                id,
                QEntry {
                    offset: resource.offset(),
                    since: SystemTime::now(),
                    op: Op::Idle(resource),
                },
            );
        }

        Self {
            started: SystemTime::now(),
            ct,
            return_tx,
            max_idle,
            max_resource_idle,
            id_counter,
            map,
            prep_errors_left: max_prep_errors + 1,
        }
    }

    fn next_id(&mut self) -> usize {
        self.id_counter += 1;
        self.id_counter
    }

    fn wait(&mut self, offset: u64, resp: Response<RM::Resource>) {
        let id = self.next_id();
        self.map.insert(
            id,
            QEntry {
                offset,
                since: SystemTime::now(),
                op: Op::Waiting(resp),
            },
        );
    }

    fn reserve(&mut self, wait_id: usize) -> Result<()> {
        if let Some(qe) = self.map.get_mut(&wait_id) {
            let op = std::mem::replace(&mut qe.op, Op::Active);
            let resp = match op {
                Op::Waiting(resp) => resp,
                _ => {
                    // put the op back
                    let _ = std::mem::replace(&mut qe.op, op);
                    bail!("invalid entry")
                }
            };
            qe.op = Op::Reserved(resp, qe.since);
            qe.since = SystemTime::now();
        } else {
            bail!("entry not found");
        }
        Ok(())
    }

    fn idle(&mut self, resource: RM::Resource) {
        let id = self.next_id();
        self.map.insert(
            id,
            QEntry {
                offset: resource.offset(),
                since: SystemTime::now(),
                op: Op::Idle(resource),
            },
        );
    }

    async fn redeem(
        &mut self,
        reservation_id: usize,
        resource: Option<RM::Resource>,
    ) -> Result<()> {
        let success = resource.is_some() && resource.as_ref().unwrap().can_reuse();

        if !success {
            if let Some(qe) = self.map.get_mut(&reservation_id) {
                // set the entry back to waiting
                let _ = qe.reset_reserved();
            }
            bail!("resource not reusable");
        }

        let mut resource = resource.unwrap();
        if let Some(qe) = self.map.get_mut(&reservation_id) {
            resource = match qe.active(resource, reservation_id, &self.return_tx) {
                Ok(()) => return Ok(()),
                Err(resource) => resource,
            };
        }
        // remove entry if invalid
        if match self.map.get(&reservation_id) {
            Some(qe) => !qe.is_valid(),
            None => false,
        } {
            self.map.remove(&reservation_id);
        }
        self.idle(resource);
        bail!("redeem failed");
    }

    fn wait_count(&self) -> usize {
        self.map
            .values()
            .filter(|v| match v.op {
                Op::Waiting(_) => true,
                _ => false,
            })
            .count()
    }

    fn is_idle(&self) -> bool {
        self.map
            .values()
            .find(|v| match v.op {
                Op::Waiting(_) | Op::Active | Op::Reserved(_, _) => true,
                Op::Idle(_) | Op::Invalid => false,
            })
            .is_none()
    }

    fn filter_sort_oldest<F>(&self, filter_fn: F) -> Vec<(SystemTime, usize, u64)>
    where
        F: Fn(&QEntry<RM::Resource>) -> bool,
    {
        self.map
            .iter()
            .filter_map(|(k, v)| {
                if filter_fn(v) {
                    Some((v.since, *k, v.offset))
                } else {
                    None
                }
            })
            .sorted_unstable_by(|a, b| a.0.cmp(&b.0))
            .collect_vec()
    }

    fn match_waiting(&mut self) -> usize {
        let waiting = self.filter_sort_oldest(|v| matches!(v.op, Op::Waiting(_)));
        let mut idle = self.filter_sort_oldest(|v| matches!(v.op, Op::Idle(_)));
        let mut matches = 0;

        for (_, id, offset) in waiting {
            if let Some((pos, _)) = idle
                .iter()
                .find_position(|(_, _, idle_offset)| &offset == idle_offset)
            {
                let (idle_since, idle_id, idle_offset) = idle.swap_remove(pos);
                // we have a match
                matches += 1;
                let mut idle_entry = self.map.remove(&idle_id).expect("invalid queue map entry");
                let waiting_entry = self.map.get_mut(&id).expect("invalid queue map entry");
                let resource = idle_entry.take_resource().expect("taking resource failed");
                if let Err(resource) = waiting_entry.active(resource, id, &self.return_tx) {
                    // activation failed
                    let valid = waiting_entry.is_valid();
                    idle_entry.op = Op::Idle(resource);
                    self.map.insert(idle_id, idle_entry);
                    idle.push((idle_since, idle_id, idle_offset));
                    if !valid {
                        self.map.remove(&id);
                    }
                }
            }
        }
        matches
    }

    fn return_resource(&mut self, id: usize, resource: RM::Resource) {
        if !resource.can_reuse() {
            tracing::debug!(
                offset = resource.offset(),
                "resource not reusable, discarding"
            );
            if self.map.remove(&id).is_none() {
                tracing::warn!(
                    "no queue entry found for id {} while discarding active handle",
                    id
                );
            }
            return;
        }

        if let Some(qe) = self.map.get_mut(&id) {
            tracing::trace!(
                offset = resource.offset(),
                "returning reusable resource to queue"
            );
            qe.since = SystemTime::now();
            qe.offset = resource.offset();
            qe.op = Op::Idle(resource);
        } else {
            tracing::warn!(
                "no queue entry found for id {} while returning reusable resource",
                id
            );
        }
    }

    fn free_idle(&mut self, id: usize) -> Option<RM::Resource> {
        let entry = self.map.remove(&id)?;
        let is_idle = match &entry.op {
            Op::Idle(_) => true,
            _ => false,
        };
        if !is_idle {
            self.map.insert(id, entry);
            return None;
        }

        Some(match entry.op {
            Op::Idle(resource) => resource,
            _ => unreachable!(),
        })
    }

    fn cleanup(&mut self) -> Vec<RM::Resource> {
        // first, remove all invalid entries
        self.map.retain(|_, v| v.is_valid());
        // then remove all expired idle entries
        let deadline = SystemTime::now() - self.max_resource_idle;
        let ids = self
            .map
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

        let resources = ids
            .iter()
            .map(|id| self.free_idle(*id))
            .flatten()
            .collect_vec();
        resources
    }

    fn next_idle_resource_expiration(&self) -> Option<SystemTime> {
        self.map
            .iter()
            .filter_map(|(_, qe)| match qe.op {
                Op::Idle(_) => Some(qe.since + self.max_resource_idle),
                _ => None,
            })
            .min()
    }
}

pub(crate) struct ActiveHandle<R: Resource + 'static> {
    id: usize,
    resource: Option<R>,
    return_tx: mpsc::Sender<(usize, R)>,
}

impl<R: Resource> AsRef<R> for ActiveHandle<R> {
    fn as_ref(&self) -> &R {
        self.resource.as_ref().unwrap()
    }
}

impl<R: Resource> AsMut<R> for ActiveHandle<R> {
    fn as_mut(&mut self) -> &mut R {
        self.resource.as_mut().unwrap()
    }
}

impl<R: Resource + 'static> Drop for ActiveHandle<R> {
    fn drop(&mut self) {
        if let Some(resource) = self.resource.take() {
            let id = self.id;
            let return_tx = self.return_tx.clone();
            tokio::spawn(async move {
                if let Err(_) = return_tx.send((id, resource)).await {
                    tracing::warn!("unable to return resource {} to queue", id);
                }
            });
        }
    }
}

pub(super) mod ctrl {
    use crate::io_scheduler::queue::{Op, QueueInner};
    use crate::io_scheduler::resource_manager::Entry;
    use crate::io_scheduler::ResourceManager;
    use anyhow::{bail, Result};
    use std::time::SystemTime;

    pub(crate) struct QueueCtrl<'a, RM: ResourceManager + Send + Sync + 'static> {
        inner: &'a mut QueueInner<RM>,
        pub(super) futures: Vec<(usize, RM::ResourceFuture)>,
        pub(super) finalize_resources: Vec<RM::Resource>,
    }

    impl<'a, RM: ResourceManager + Send + Sync + 'static> QueueCtrl<'a, RM> {
        pub(super) fn new(inner: &'a mut QueueInner<RM>) -> Self {
            Self {
                inner,
                futures: vec![],
                finalize_resources: vec![],
            }
        }

        pub fn entries(&self) -> Vec<Entry> {
            let mut entries: Vec<Entry> = vec![];

            for (k, v) in self.inner.map.iter() {
                match v.op {
                    Op::Waiting(_) => entries.push(
                        Waiting {
                            id: *k,
                            offset: v.offset,
                            since: v.since,
                        }
                        .into(),
                    ),
                    Op::Active => entries.push(
                        Active {
                            offset: v.offset,
                            since: v.since,
                        }
                        .into(),
                    ),
                    Op::Idle(_) => entries.push(
                        Idle {
                            id: *k,
                            since: v.since,
                            offset: v.offset,
                        }
                        .into(),
                    ),
                    Op::Reserved(_, _) => entries.push(
                        Active {
                            offset: v.offset,
                            since: v.since,
                        }
                        .into(),
                    ),
                    Op::Invalid => {}
                }
            }

            entries.sort_unstable_by(|a, b| a.offset().cmp(&b.offset()));

            entries
        }

        pub fn prepare(&mut self, waiting: &Waiting, fut: RM::ResourceFuture) -> Result<()> {
            self.inner.reserve(waiting.id)?;
            self.futures.push((waiting.id, fut));
            Ok(())
        }

        pub fn take_idle(&mut self, idle: &Idle) -> Option<RM::Resource> {
            self.inner.free_idle(idle.id)
        }

        pub fn finalize(&mut self, idle: &Idle) -> Result<()> {
            if let Some(res) = self.take_idle(idle) {
                self.finalize_resources.push(res);
                Ok(())
            } else {
                bail!("unable to finalize idle resource, take_idle failed");
            }
        }
    }

    #[derive(Clone, PartialEq, Eq)]
    pub struct Idle {
        id: usize,
        pub offset: u64,
        pub since: SystemTime,
    }

    #[derive(Clone, PartialEq, Eq)]
    pub struct Waiting {
        id: usize,
        pub offset: u64,
        pub since: SystemTime,
    }

    #[derive(Clone, PartialEq, Eq)]
    pub struct Active {
        pub offset: u64,
        pub since: SystemTime,
    }
}
