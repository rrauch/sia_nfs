use crate::io_scheduler::resource_manager::Action::Sleep;
use crate::io_scheduler::resource_manager::{
    Action, Context, QueueCtrl, Resource, ResourceManager,
};
use crate::io_scheduler::Scheduler;
use crate::vfs::file_writer::FileWriter;
use crate::vfs::Vfs;
use anyhow::{anyhow, bail};
use futures_util::future::BoxFuture;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct Upload {
    vfs: Arc<Vfs>,
}

impl Upload {
    pub(crate) fn new(vfs: Arc<Vfs>, max_idle: Duration) -> Scheduler<Self> {
        Scheduler::new(
            Upload { vfs },
            false,
            max_idle + Duration::from_millis(10),
            max_idle,
            0,
        )
    }
}

impl ResourceManager for Upload {
    type Resource = FileWriter;
    type PreparationKey = (u64, String);
    type AccessKey = u64;
    type ResourceData = ();
    type ResourceFuture = BoxFuture<'static, anyhow::Result<Self::Resource>>; // nonexistent, actually

    async fn prepare(
        &self,
        preparation_key: &Self::PreparationKey,
    ) -> anyhow::Result<(Self::AccessKey, Self::ResourceData, Vec<Self::Resource>)> {
        let (parent_id, name) = preparation_key;
        let parent = self
            .vfs
            .inode_by_id((*parent_id).into())
            .await?
            .ok_or(anyhow!("parent not found"))?;

        let parent = parent
            .as_parent()
            .ok_or(anyhow!("inode cannot have children"))?;

        let fw = self.vfs.write_file(parent, name.to_string()).await?;
        let file = fw.to_file();

        tracing::debug!(
            file_id = %file.id(),
            file_name = file.name(),
            "upload prepared"
        );

        Ok((file.id().value(), (), vec![fw]))
    }

    fn process(
        &self,
        queue: &mut QueueCtrl<Self>,
        _: &mut Self::ResourceData,
        _: &Context,
    ) -> anyhow::Result<Action> {
        let active_count = queue
            .entries()
            .iter()
            .filter(|e| e.as_idle().is_some() || e.as_active().is_some())
            .count();

        if active_count != 1 {
            bail!(
                "expected active_count to be 1 but found {}, aborting",
                active_count
            );
        };

        Ok(Sleep(Duration::from_secs(5)))
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
