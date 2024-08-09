use crate::io_scheduler::{Entry, Backend, BackendTask, Scheduler, Lease};
use crate::vfs::{File, FileWriter, Inode, Vfs};
use anyhow::bail;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) struct Upload {
    vfs: Arc<Vfs>,
}

impl Upload {
    pub(crate) fn new(vfs: Arc<Vfs>, max_idle: Duration) -> Scheduler<Self> {
        Scheduler::new(Upload { vfs }, max_idle, false)
    }
}

impl Backend for Upload {
    type Task = FileWriter;
    type Key = (u64, String);

    async fn begin(&self, key: &Self::Key) -> anyhow::Result<(File, Vec<Self::Task>)> {
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

    async fn lease(
        &self,
        entry: Arc<Mutex<Entry<Self>>>,
        offset: u64,
    ) -> anyhow::Result<Lease<Self>> {
        let mut watch_rx = { entry.lock().unwrap().status_tx.subscribe() };
        loop {
            // try to get one right now
            {
                let mut lock = entry.lock().unwrap();
                if let Some(lease) = lock.lease(&entry, offset) {
                    return Ok(lease);
                }
            }

            // wait for an update with the exact offset
            while !watch_rx.borrow_and_update().idle_offsets.contains(&offset) {
                watch_rx.changed().await?;
            }
        }
    }
}

impl BackendTask for FileWriter {
    fn offset(&self) -> u64 {
        self.bytes_written()
    }

    async fn finalize(self) -> anyhow::Result<()> {
        let _ = self.finalize().await?;
        Ok(())
    }
}
