use crate::io_scheduler::{Action, ResourceManager, Resource, QueueState, Scheduler};
use crate::vfs::file_writer::FileWriter;
use crate::vfs::inode::Inode;
use crate::vfs::Vfs;
use anyhow::bail;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct Upload {
    vfs: Arc<Vfs>,
}

impl Upload {
    pub(crate) fn new(vfs: Arc<Vfs>, max_idle: Duration) -> Scheduler<Self> {
        Scheduler::new(Upload { vfs }, false, max_idle, max_idle, 0)
    }
}

impl ResourceManager for Upload {
    type Resource = FileWriter;
    type PreparationKey = (u64, String);
    type AccessKey = u64;
    type ResourceData = ();
    type AdviseData = ();

    async fn prepare(
        &self,
        preparation_key: &Self::PreparationKey,
    ) -> anyhow::Result<(
        Self::AccessKey,
        Self::ResourceData,
        Self::AdviseData,
        Vec<Self::Resource>,
    )> {
        let (parent_id, name) = preparation_key;
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

        Ok((file.id(), (), (), vec![fw]))
    }

    async fn new_resource(&self, _offset: u64, _data: &Self::ResourceData) -> anyhow::Result<Self::Resource> {
        bail!("upload queues cannot start new resources")
    }

    fn advise<'a>(
        &self,
        _state: &'a QueueState,
        _data: &mut Self::AdviseData,
    ) -> anyhow::Result<(Duration, Option<Action<'a>>)> {
        let next_consultation = Duration::from_secs(10);
        Ok((next_consultation, None))
    }
}

impl Resource for FileWriter {
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
