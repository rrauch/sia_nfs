use crate::io_scheduler::queue::{ActiveHandle, Activity, Queue, WaitHandle};
use crate::io_scheduler::{Backend, BackendTask, Scheduler};
use crate::vfs::file_reader::FileReader;
use crate::vfs::inode::{File, Inode};
use crate::vfs::Vfs;

use anyhow::bail;
use anyhow::Result;
use futures_util::AsyncSeekExt;
use itertools::Either;
use std::cmp::min;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::broadcast;
use tracing::instrument;

pub(crate) struct Download {
    vfs: Arc<Vfs>,
    max_downloads: NonZeroUsize,
    max_wait_for_match: Duration,
    max_inactivity_for_match: Duration,
    initial_idle: Duration,
    max_idle: Duration,
}

impl Download {
    pub(crate) fn new(
        vfs: Arc<Vfs>,
        initial_idle: Duration,
        max_idle: Duration,
        max_downloads: NonZeroUsize,
        max_wait_for_match: Duration,
        max_inactivity_for_match: Duration,
    ) -> Scheduler<Self> {
        let downloader = Download {
            vfs,
            max_downloads,
            max_wait_for_match,
            max_inactivity_for_match,
            initial_idle,
            max_idle,
        };

        Scheduler::new(downloader, true)
    }

    async fn file(&self, file_id: u64) -> anyhow::Result<File> {
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

    async fn download(&self, file_id: u64, offset: u64) -> Result<FileReader> {
        let file = self.file(file_id).await?;
        tracing::debug!(
            id = file.id(),
            name = file.name(),
            offset,
            "initiating new download"
        );
        let mut file_reader = self.vfs.read_file(&file).await?;
        file_reader.seek(SeekFrom::Start(offset)).await?;
        Ok(file_reader)
    }

    async fn try_resume_download<BT: BackendTask>(
        &self,
        offset: u64,
        queue: &Arc<Queue<BT>>,
    ) -> Result<Either<ActiveHandle<BT>, (WaitHandle<BT>, broadcast::Receiver<Activity>, u64)>>
    {
        let wait_deadline = SystemTime::now() + self.max_wait_for_match;
        // check if there are any active downloads or reservations that could become potentially interesting
        let (mut wait_handle, mut activity_rx, file_id, mut last_activity, mut contains_candidate) = {
            let mut guard = queue.lock();
            let mut wait_handle = guard.wait(offset);
            let file_id = queue.file().borrow().id();

            // first, try to resume right now
            wait_handle = match guard.resume(wait_handle) {
                Either::Left(active_handle) => {
                    tracing::trace!("immediate download resumption");
                    return Ok(Either::Left(active_handle));
                }
                Either::Right(wait_handle) => wait_handle,
            };

            let activity = queue.activity();
            if offset == 0 {
                // no need to wait further in this case
                return Ok(Either::Right((wait_handle, activity, file_id)));
            }
            let contains_candidate = guard.contains_candidate(&wait_handle);
            (
                wait_handle,
                activity,
                file_id,
                queue.last_activity(),
                contains_candidate,
            )
        };

        loop {
            // making sure it doesn't exceed total wait time
            let inactivity_deadline =
                min(last_activity + self.max_inactivity_for_match, wait_deadline);

            let sleep_deadline = if contains_candidate {
                // if there is a potential candidate we wait longer
                wait_deadline
            } else {
                inactivity_deadline
            };

            let sleep_duration = sleep_deadline
                .duration_since(SystemTime::now())
                .unwrap_or_else(|_| Duration::from_secs(0));

            tokio::select! {
                _ = tokio::time::sleep(sleep_duration) => {
                    tracing::trace!("waiting timeout reached");
                    return Ok(Either::Right((wait_handle, activity_rx, file_id)));
                },
                res = activity_rx.recv() => {
                    let activity = res?;
                    last_activity = activity.timestamp();
                    let mut queue = queue.lock();
                    if activity.offset() == offset {
                        if let Activity::Idle(_, _) = activity {
                            // we want this, try to get it
                            wait_handle = match queue.resume(wait_handle) {
                                Either::Left(active_handle) => {
                                    // success
                                    tracing::trace!("suitable idle task became available, resuming");
                                    return Ok(Either::Left(active_handle));
                                }
                                Either::Right(wait_handle) => {
                                    // we missed it
                                    wait_handle
                                }
                            };
                        }
                    };
                    contains_candidate = queue.contains_candidate(&wait_handle);
                }
            }
        }
    }
}

impl Backend for Download {
    type Task = FileReader;
    type Key = u64;

    #[instrument[skip(self)]]
    async fn begin(&self, key: &Self::Key) -> anyhow::Result<Queue<Self::Task>> {
        let file = self.file(*key).await?;
        tracing::debug!(
            file_id = file.id(),
            file_name = file.name(),
            "download prepared"
        );
        Ok(Queue::new(
            self.max_downloads,
            self.max_idle,
            SystemTime::now() + self.initial_idle,
            vec![],
            file,
        ))
    }

    #[allow(private_interfaces)]
    async fn acquire(
        &self,
        queue: Arc<Queue<Self::Task>>,
        offset: u64,
    ) -> Result<ActiveHandle<Self::Task>> {
        tracing::trace!("begin acquiring handle");

        // Phase 1: try to resume a download
        let (wait_handle, mut activity_rx, file_id) =
            match self.try_resume_download(offset, &queue).await? {
                Either::Left(active_handle) => {
                    return Ok(active_handle);
                }
                Either::Right(wait_handle) => wait_handle,
            };

        // Phase 2: Start a new download
        let mut handle = Either::Right(wait_handle);
        loop {
            match handle {
                Either::Left(reserve_handle) => {
                    // start a new download
                    let file_reader = self.download(file_id, offset).await?;
                    let mut queue = queue.lock();
                    tracing::trace!("redeeming reservation");
                    return Ok(queue.redeem(reserve_handle, file_reader));
                }
                Either::Right(wait_handle) => {
                    match {
                        let mut queue = queue.lock();
                        // first, try to reserve a slot directly
                        let wait_handle = match queue.reserve(wait_handle) {
                            Either::Left(reserve_handle) => {
                                // good, we got a reserve slot
                                // skip to next iteration to start download
                                tracing::trace!("new slot reserved");
                                handle = Either::Left(reserve_handle);
                                continue;
                            }
                            Either::Right(wait_handle) => wait_handle,
                        };
                        // unfortunately, no free slot could be reserved
                        // next, try to free an idle slot
                        let res = queue.try_free(wait_handle);
                        // avoid unnecessary work by marking the current state as seen
                        activity_rx = activity_rx.resubscribe();
                        res
                    } {
                        Either::Left(freed_handle) => {
                            // an idle slot has been freed for this reserved slot
                            let reserved_handle = freed_handle.finalize().await?;
                            tracing::trace!("freed download and acquired reservation");
                            handle = Either::Left(reserved_handle);
                        }
                        Either::Right(wait_handle) => {
                            // everything is busy and full, try again once something changes
                            handle = Either::Right(wait_handle);
                            let _ = activity_rx.recv().await?;
                        }
                    }
                }
            }
        }
    }
}

impl BackendTask for FileReader {
    fn offset(&self) -> u64 {
        self.offset()
    }

    fn can_reuse(&self) -> bool {
        !self.eof() && self.error_count() == 0
    }

    async fn finalize(self) -> anyhow::Result<()> {
        Ok(())
    }

    fn to_file(&self) -> File {
        self.file().clone()
    }
}
