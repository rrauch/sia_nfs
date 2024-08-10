use crate::io_scheduler::{
    Activity, Backend, BackendTask, Entry, Lease, Reservation, Scheduler, TasksStatus,
};
use crate::vfs::{File, FileReader, Inode, Vfs};
use anyhow::bail;
use futures_util::AsyncSeekExt;
use std::cmp::min;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tracing::instrument;

pub(crate) struct Download {
    vfs: Arc<Vfs>,
    max_downloads: usize,
    max_wait_for_match: Duration,
    max_inactivity_for_match: Duration,
}

impl Download {
    pub(crate) fn new(
        vfs: Arc<Vfs>,
        max_idle: Duration,
        max_downloads: NonZeroUsize,
        max_wait_for_match: Duration,
        max_inactivity_for_match: Duration,
    ) -> Scheduler<Self> {
        let downloader = Download {
            vfs,
            max_downloads: max_downloads.get(),
            max_wait_for_match,
            max_inactivity_for_match,
        };

        Scheduler::new(downloader, max_idle, true)
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

    async fn download(
        &self,
        reservation: Reservation<Download>,
        file_id: u64,
        offset: u64,
    ) -> anyhow::Result<Lease<Download>> {
        let file = self.file(file_id).await?;
        tracing::debug!(
            id = file.id(),
            name = file.name(),
            offset,
            "initiating new download"
        );
        let mut file_reader = self.vfs.read_file(&file).await?;
        file_reader.seek(SeekFrom::Start(offset)).await?;
        Ok(reservation.redeem(file_reader)?)
    }

    fn interested(&self, status: &TasksStatus, offset: u64) -> bool {
        status
            .idle_offsets
            .iter()
            .chain(status.active_offsets.iter())
            .chain(status.reserved_offsets.iter())
            .find(|n| n <= &&offset)
            .is_some()
    }
}

impl Backend for Download {
    type Task = FileReader;
    type Key = u64;

    #[instrument[skip(self)]]
    async fn begin(&self, key: &Self::Key) -> anyhow::Result<(File, Vec<Self::Task>)> {
        let file = self.file(*key).await?;
        tracing::debug!(
            file_id = file.id(),
            file_name = file.name(),
            "download prepared"
        );
        Ok((file, vec![]))
    }

    #[instrument(skip(self, entry))]
    async fn lease(
        &self,
        entry: Arc<Mutex<Entry<Self>>>,
        offset: u64,
    ) -> anyhow::Result<Lease<Self>> {
        let wait_deadline = SystemTime::now() + self.max_wait_for_match;
        tracing::trace!("begin getting lease");
        let (mut watch_rx, file_id) = {
            let lock = entry.lock().unwrap();
            (lock.status_tx.subscribe(), lock.key)
        };

        // Phase 1: try to wait for a suitable download to resume
        'outer: while SystemTime::now() < wait_deadline {
            // check if there are any active downloads or reservations that could become potentially interesting
            let mut status = {
                let mut lock = entry.lock().unwrap();

                // first, try to get a lease right now
                if let Some(lease) = lock.lease(&entry, offset) {
                    return Ok(lease);
                }

                // then get the current status
                let status = lock.status();
                // avoid unnecessary work by marking the current state as seen
                watch_rx.mark_unchanged();
                status
            };

            loop {
                if !self.interested(&status, offset) {
                    // nothing interesting queued right now
                    // no need to wait further, start a new download
                    tracing::trace!("not interested in existing tasks");
                    break 'outer;
                }

                let sleep_deadline = match &status.activity {
                    Activity::Active => wait_deadline,
                    Activity::Idle(None) => SystemTime::now(),
                    Activity::Idle(Some(last_activity)) => {
                        *last_activity + self.max_inactivity_for_match
                    }
                };
                // making sure it doesn't exceed total wait time
                let sleep_deadline = min(sleep_deadline, wait_deadline);

                let sleep_duration = sleep_deadline
                    .duration_since(SystemTime::now())
                    .unwrap_or_else(|_| Duration::from_secs(0));

                tokio::select! {
                    _ = tokio::time::sleep(sleep_duration) => {
                        tracing::trace!("waiting timeout reached");
                        break 'outer;
                    },
                    _ = watch_rx.changed() => {
                        status = watch_rx.borrow_and_update().clone();
                        if status.idle_offsets.contains(&offset) {
                            tracing::trace!("suitable idle task became available");
                            break;
                        }
                    }
                }
            }
        }

        // Phase 2: Start a new download
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
                        let reservation = lock.make_reservation(&entry, offset);
                        Some(reservation)
                    } else {
                        None
                    }
                }
            } {
                // managed to get a reservation
                // start a new download
                return self.download(reservation, file_id, offset).await;
            }

            let mut freed_task = None;
            // try to remove the oldest idle download
            {
                let mut lock = entry.lock().unwrap();
                if lock.tasks.len() >= self.max_downloads {
                    // remove the oldest idle download
                    freed_task = lock.free_task();
                }
                if lock.tasks.len() < self.max_downloads {
                    // free_task seems to have worked
                    // get a reservation and try downloading again
                    reservation = Some(lock.make_reservation(&entry, offset));
                }

                // avoid unnecessary work by marking the current state as seen
                watch_rx.mark_unchanged();
            }
            // don't forget to finalize the freed task (if we have one)
            if let Some(task) = freed_task.take() {
                tracing::trace!("finalizing freed task");
                task.finalize().await?;
            }

            if reservation.is_none() {
                // everything is busy and full, try again once something changes
                watch_rx.changed().await?
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
}
