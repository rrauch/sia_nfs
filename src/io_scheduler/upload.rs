use crate::io_scheduler::queue::{ActiveHandle, Activity, Queue};
use crate::io_scheduler::{Backend, BackendTask, Scheduler};
use crate::vfs::file_writer::FileWriter;
use crate::vfs::inode::Inode;
use crate::vfs::Vfs;
use anyhow::bail;
use itertools::Either;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub(crate) struct Upload {
    vfs: Arc<Vfs>,
    initial_idle: Duration,
    max_idle: Duration,
}

impl Upload {
    pub(crate) fn new(
        vfs: Arc<Vfs>,
        initial_idle: Duration,
        max_idle: Duration,
    ) -> Scheduler<Self> {
        Scheduler::new(
            Upload {
                vfs,
                initial_idle,
                max_idle,
            },
            false,
        )
    }
}

impl Backend for Upload {
    type Task = FileWriter;
    type Key = (u64, String);

    #[allow(private_interfaces)]
    async fn begin(&self, key: &Self::Key) -> anyhow::Result<Queue<Self::Task>> {
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

        let fw = self.vfs.write_file(&parent, name.to_string()).await?;
        let file = fw.to_file();

        tracing::debug!(
            file_id = file.id(),
            file_name = file.name(),
            "upload prepared"
        );

        let queue = Queue::new(
            file.id(),
            NonZeroUsize::new(1).unwrap(),
            self.max_idle,
            SystemTime::now() + self.initial_idle,
            vec![fw],
        );

        Ok(queue)
    }

    #[allow(private_interfaces)]
    async fn acquire(
        &self,
        queue: Arc<Queue<Self::Task>>,
        offset: u64,
    ) -> anyhow::Result<ActiveHandle<Self::Task>> {
        tracing::trace!("begin acquiring handle");
        let (mut wait_handle, mut activity) = {
            let mut quard = queue.lock();
            (quard.wait(offset), queue.activity())
        };
        loop {
            {
                let mut queue = queue.lock();
                match queue.resume(wait_handle) {
                    Either::Left(active_handle) => {
                        tracing::trace!("upload resumption");
                        return Ok(active_handle);
                    }
                    Either::Right(h) => {
                        wait_handle = h;
                        activity = activity.resubscribe();
                    }
                }
            }
            tracing::trace!("waiting for upload progress");
            loop {
                let activity = activity.recv().await?;
                if activity.offset() == offset {
                    if let Activity::Idle(..) = activity {
                        tracing::trace!("suitable idle task became available, resuming");
                        break;
                    }
                }
            }
        }
    }
}

impl BackendTask for FileWriter {
    fn offset(&self) -> u64 {
        self.bytes_written()
    }

    fn can_reuse(&self) -> bool {
        !self.is_closed() && self.error_count() == 0
    }

    async fn finalize(self) -> anyhow::Result<()> {
        let _ = self.finalize().await?;
        Ok(())
    }
}
