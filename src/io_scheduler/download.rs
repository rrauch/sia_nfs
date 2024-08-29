use crate::io_scheduler::{Action, QueueState, Resource, ResourceManager, Scheduler};
use crate::vfs::file_reader::FileReader;
use crate::vfs::inode::{File, Inode};
use crate::vfs::Vfs;

use crate::io_scheduler::strategy::DownloadStrategy;
use anyhow::bail;
use anyhow::Result;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;

pub(crate) struct Download {
    vfs: Arc<Vfs>,
    download_strategy: DownloadStrategy,
}

impl Download {
    pub(crate) fn new(
        vfs: Arc<Vfs>,
        max_queue_idle: Duration,
        max_resource_idle: Duration,
        max_downloads: NonZeroUsize,
        max_wait_for_match: Duration,
        min_wait_for_match: Duration,
    ) -> Scheduler<Self> {
        let downloader = Download {
            vfs,
            download_strategy: DownloadStrategy::new(
                max_downloads.get(),
                max_wait_for_match,
                min_wait_for_match,
            ),
        };

        Scheduler::new(downloader, true, max_queue_idle, max_resource_idle, 2)
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
        Ok(self.vfs.read_file(&file, offset).await?)
    }
}

impl ResourceManager for Download {
    type Resource = FileReader;
    type PreparationKey = u64;
    type AccessKey = u64;
    type ResourceData = u64;
    type AdviseData = ();

    #[instrument[skip(self)]]
    async fn prepare(
        &self,
        preparation_key: &Self::PreparationKey,
    ) -> Result<(
        Self::AccessKey,
        Self::ResourceData,
        Self::AdviseData,
        Vec<Self::Resource>,
    )> {
        let file = self.file(*preparation_key).await?;
        tracing::debug!(
            file_id = file.id(),
            file_name = file.name(),
            "download prepared"
        );
        Ok((file.id(), file.id(), (), vec![]))
    }

    async fn new_resource(&self, offset: u64, data: &Self::ResourceData) -> Result<Self::Resource> {
        let file_id = *data;
        self.download(file_id, offset).await
    }

    fn advise<'a>(
        &self,
        state: &'a QueueState,
        _data: &mut Self::AdviseData,
    ) -> Result<(Duration, Option<Action<'a>>)> {
        self.download_strategy.advise(state)
    }
}

impl Resource for FileReader {
    fn offset(&self) -> u64 {
        self.offset()
    }

    fn can_reuse(&self) -> bool {
        !self.eof() && self.error_count() == 0
    }

    async fn finalize(self) -> Result<()> {
        Ok(())
    }
}
