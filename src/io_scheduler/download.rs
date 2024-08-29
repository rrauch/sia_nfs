use crate::io_scheduler::{Action, QueueState, Resource, ResourceManager, Scheduler};
use crate::vfs::file_reader::FileReader;
use crate::vfs::inode::{File, Inode};
use crate::vfs::Vfs;

use anyhow::bail;
use anyhow::Result;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::instrument;

pub(crate) struct Download {
    vfs: Arc<Vfs>,
    max_active: usize,
    max_wait_for_match: Duration,
    min_wait_for_match: Duration,
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
            max_active: max_downloads.get(),
            max_wait_for_match,
            min_wait_for_match,
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
        assert!(!state.waiting.is_empty());
        // first, check if we need to free idle resources
        if state.active.len() + state.preparing.len() + state.idle.len() >= self.max_active {
            // find the resource that has been idle the longest
            if let Some(idle) = state.idle.iter().min_by_key(|i| i.since) {
                return Ok((Duration::from_millis(0), Some(Action::Free(idle))));
            } else {
                // nothing we can do now
                return Ok((Duration::from_millis(250), None));
            }
        }

        // get the wait resource with the lowest offset
        let waiting = state.waiting.iter().min_by_key(|w| w.offset).unwrap();
        if waiting.offset == 0 {
            // no need to wait here
            return Ok((
                Duration::from_millis(250),
                Some(Action::NewResource(waiting)),
            ));
        }
        let wait_duration = SystemTime::now()
            .duration_since(waiting.since)
            .unwrap_or_default();
        if wait_duration < self.min_wait_for_match {
            let next_try_in = (self.min_wait_for_match - wait_duration) + Duration::from_millis(1);
            // check later
            return Ok((next_try_in, None));
        }

        if state.active.get_before_offset(waiting.offset).count() == 0 {
            // start new download now
            return Ok((
                Duration::from_millis(250),
                Some(Action::NewResource(waiting)),
            ));
        }

        let wait_duration = SystemTime::now()
            .duration_since(waiting.since)
            .unwrap_or_default();
        if wait_duration < self.max_wait_for_match {
            let next_try_in = (self.max_wait_for_match - wait_duration) + Duration::from_millis(1);
            // check later
            return Ok((next_try_in, None));
        }

        Ok((
            Duration::from_millis(250),
            Some(Action::NewResource(waiting)),
        ))
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
