use super::{Action, QueueState, Resource, ResourceManager};
use anyhow::{bail, Result};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use std::cmp::min;
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
        advise_data: RM::AdviseData,
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
            let mut queue = QueueImpl::new(
                resource_manager,
                resource_data,
                advise_data,
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
                queue.run(resource_rx, return_rx, term_fn).await;
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
}

struct QueueImpl<RM: ResourceManager + Send + Sync + 'static>
where
    <RM as ResourceManager>::Resource: 'static,
{
    resource_manager: Arc<RM>,
    resource_data: Arc<RM::ResourceData>,
    advise_data: RM::AdviseData,
    ct: CancellationToken,
    return_tx: mpsc::Sender<(usize, RM::Resource)>,
    max_idle: Duration,
    max_resource_idle: Duration,
    id_counter: usize,
    map: HashMap<usize, QEntry<RM::Resource>>,
    prep_errors_left: usize,
}

impl<RM: ResourceManager + Send + Sync + 'static> QueueImpl<RM> {
    fn new(
        resource_manager: Arc<RM>,
        resource_data: RM::ResourceData,
        advise_data: RM::AdviseData,
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
            resource_manager,
            resource_data: Arc::new(resource_data),
            advise_data,
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

    fn stats(&self) -> QueueState {
        let mut idle = vec![];
        let mut waiting = vec![];
        let mut active = vec![];
        let mut preparing = vec![];

        for (k, v) in self.map.iter() {
            match v.op {
                Op::Waiting(_) => waiting.push(super::Waiting {
                    id: *k,
                    offset: v.offset,
                    since: v.since,
                }),
                Op::Active => active.push(super::Active { offset: v.offset }),
                Op::Idle(_) => idle.push(super::Idle {
                    id: *k,
                    since: v.since,
                    offset: v.offset,
                }),
                Op::Reserved(_, _) => preparing.push(super::Preparing { offset: v.offset }),
                Op::Invalid => {}
            }
        }

        QueueState::new(idle, waiting, active, preparing)
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

    async fn run(
        &mut self,
        mut resource_rx: mpsc::Receiver<(u64, Response<RM::Resource>)>,
        mut return_rx: mpsc::Receiver<(usize, RM::Resource)>,
        mut term_fn: Option<Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>>,
    ) {
        let mut last_activity = SystemTime::now();
        let mut prepare_resources = JoinSet::new();
        // add a dummy entry that never resolves
        prepare_resources.spawn(async move {
            // wait forever
            future::pending::<()>().await;
            unreachable!()
        });
        let mut finalize_resources: JoinSet<()> = JoinSet::new();

        loop {
            // cleanup
            for resource in self.cleanup() {
                finalize_resources.spawn(async move {
                    let _ = resource.finalize().await;
                });
            }

            // match waiting & idle first
            if self.match_waiting() > 0 {
                last_activity = SystemTime::now();
            }

            let mut next_advise = SystemTime::now() + Duration::from_secs(86400);
            if self.wait_count() > 0 {
                // work needs to be done here
                let stats = self.stats();
                let (next_advise_reply, action) = match self
                    .resource_manager
                    .advise(&stats, &mut self.advise_data)
                {
                    Ok((next_advise, action)) => (SystemTime::now() + next_advise, action),
                    Err(err) => {
                        tracing::error!(error = %err, "error getting resource_manager advise, shutting down queue");
                        break;
                    }
                };
                next_advise = next_advise_reply;
                if let Some(action) = action {
                    match action {
                        Action::Free(idle) => {
                            if let Some(resource) = self.free_idle(idle.id) {
                                finalize_resources.spawn(async move {
                                    let _ = resource.finalize().await;
                                });
                            }
                        }
                        Action::NewResource(waiting) => {
                            let id = waiting.id;
                            let offset = waiting.offset;
                            let backend = self.resource_manager.clone();
                            let data = self.resource_data.clone();
                            if let Ok(()) = self.reserve(id) {
                                prepare_resources.spawn(async move {
                                    (id, backend.new_resource(offset, &data).await)
                                });
                            } else {
                                tracing::error!("error reserving queue slot for {}", id);
                            }
                        }
                    }
                }
            }

            let now = SystemTime::now();

            let next_advise_in = next_advise.duration_since(now).unwrap_or_default();

            let resource_inactivity_timeout_in = self
                .next_idle_resource_expiration()
                .map_or(Duration::from_secs(86400 * 365), |exp| {
                    exp.duration_since(now).unwrap_or_default()
                });

            let next_run_in = min(resource_inactivity_timeout_in, next_advise_in);

            let inactivity_timeout_in = if self.is_idle() {
                (last_activity + self.max_idle)
                    .duration_since(now)
                    .unwrap_or_default()
            } else {
                Duration::from_secs(86400 * 365)
            };

            tokio::select! {
                Some((offset, resp)) = resource_rx.recv() => {
                    self.wait(offset, resp);
                    last_activity = SystemTime::now();
                },
                Some((id, resource)) = return_rx.recv() => {
                    self.return_resource(id, resource);
                    last_activity = SystemTime::now();
                }
                Some(res) = prepare_resources.join_next() => {
                    match res {
                        Ok((id, Ok(resource))) => {
                            if let Err(err) = self.redeem(id, Some(resource)).await {
                                tracing::error!(error = %err, "unable to redeem resource");
                            }
                        }
                        Ok((id, Err(err))) => {
                            self.prep_errors_left -= 1;
                            if self.prep_errors_left == 0 {
                                tracing::error!("too many preparation errors, shutting down queue");
                                break;
                            }
                            let _ = self.redeem(id, None);
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
                _ = self.ct.cancelled() => {
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
        for (_, v) in self.map.drain() {
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
