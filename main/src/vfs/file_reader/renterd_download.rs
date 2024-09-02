use crate::io_scheduler::strategy::DownloadStrategy;
use crate::io_scheduler::{Action, QueueState, Resource, ResourceManager, Scheduler};
use crate::ReadStream;
use anyhow::Result;
use anyhow::{anyhow, bail};
use futures::{AsyncRead, AsyncSeek};
use renterd_client::worker::object::DownloadableObject;
use renterd_client::Client;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tracing::instrument;

const VERSION_HASH_SEED: u64 = 3986469829483842332;

pub(super) struct RenterdDownload {
    renterd: Client,
    download_strategy: DownloadStrategy,
    download_limiter: Arc<Semaphore>,
}

impl RenterdDownload {
    pub(super) fn new(
        renterd: Client,
        max_concurrent_downloads: NonZeroUsize,
        max_queue_idle: Duration,
        max_resource_idle: Duration,
        max_downloads: NonZeroUsize,
        max_wait_for_match: Duration,
        min_wait_for_match: Duration,
    ) -> Scheduler<Self> {
        let download_limiter = Arc::new(Semaphore::new(max_concurrent_downloads.get()));
        let renterd_download = Self {
            renterd,
            download_strategy: DownloadStrategy::new(
                max_downloads.get(),
                max_wait_for_match,
                min_wait_for_match,
            ),
            download_limiter,
        };

        Scheduler::new(renterd_download, true, max_queue_idle, max_resource_idle, 2)
    }

    async fn download(
        &self,
        known_dl: &DownloadableObject,
        known_version: &Version,
        offset: u64,
    ) -> Result<ObjectReader> {
        tracing::trace!("waiting for download permit");
        let download_permit = timeout(
            Duration::from_secs(60),
            self.download_limiter.clone().acquire_owned(),
        )
        .await??;
        tracing::trace!("download permit acquired");

        let (new_dl, new_version) = self
            .dl_object(
                known_dl.bucket.as_ref().unwrap().to_string(),
                &known_dl.path,
            )
            .await?;

        if &new_version != known_version {
            bail!("file version has changed, cannot continue");
        }

        tracing::trace!(
            bucket = new_dl.bucket,
            path = new_dl.path,
            offset = offset,
            "opening new stream"
        );

        let stream = new_dl.open_seekable_stream(offset).await?;

        tracing::debug!(
            bucket = new_dl.bucket,
            path = new_dl.path,
            offset = offset,
            "new object_reader created"
        );

        Ok(ObjectReader {
            bucket: new_dl.bucket,
            path: new_dl.path,
            offset,
            size: new_dl.length.unwrap(),
            error_count: 0,
            stream: Box::new(stream),
            _download_permit: download_permit,
        })
    }

    async fn dl_object(&self, bucket: String, path: &str) -> Result<(DownloadableObject, Version)> {
        let dl_object = self
            .renterd
            .worker()
            .object()
            .download(path, Some(bucket))
            .await?
            .ok_or(anyhow!("renterd couldn't find the file"))?;

        if !dl_object.seekable {
            bail!("object is not seekable");
        }

        dl_object.length.ok_or(anyhow!("file size is unknown"))?;

        let version = Version::from(&dl_object);

        Ok((dl_object, version))
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Version(u128);

impl From<&Version> for Vec<u8> {
    fn from(value: &Version) -> Self {
        value.0.to_le_bytes().to_vec()
    }
}

impl From<&DownloadableObject> for Version {
    fn from(dl_object: &DownloadableObject) -> Self {
        // derive version from available object metadata
        let mut hasher = xxhash_rust::xxh3::Xxh3::with_seed(VERSION_HASH_SEED);
        hasher.update("renterd object version v1 start\n".as_bytes());
        hasher.update("length:".as_bytes());
        hasher.update(dl_object.length.unwrap().to_le_bytes().as_slice());
        hasher.update("\netag:".as_bytes());
        if let Some(etag) = dl_object.etag.as_ref() {
            hasher.update(etag.as_bytes());
        } else {
            hasher.update("None".as_bytes());
        }
        hasher.update("\nlast_modified:".as_bytes());
        if let Some(last_modified) = dl_object.last_modified.as_ref() {
            hasher.update(last_modified.timestamp_millis().to_le_bytes().as_slice());
        } else {
            hasher.update("None".as_bytes());
        }
        hasher.update("\ncontent_type:".as_bytes());
        if let Some(content_type) = dl_object.content_type.as_ref() {
            hasher.update(content_type.as_bytes());
        } else {
            hasher.update("None".as_bytes());
        }
        hasher.update("\nrenterd object version v1 end".as_bytes());
        Version(hasher.digest128())
    }
}

impl ResourceManager for RenterdDownload {
    type Resource = ObjectReader;
    type PreparationKey = (String, String);
    type AccessKey = (String, String, Version);
    type ResourceData = (DownloadableObject, Version);
    type AdviseData = ();

    async fn prepare(
        &self,
        preparation_key: &Self::PreparationKey,
    ) -> Result<(
        Self::AccessKey,
        Self::ResourceData,
        Self::AdviseData,
        Vec<Self::Resource>,
    )> {
        let (bucket, path) = preparation_key;
        let (dl_object, version) = self.dl_object(bucket.clone(), path).await?;

        Ok((
            (bucket.clone(), path.clone(), version.clone()),
            (dl_object, version),
            (),
            vec![],
        ))
    }

    async fn new_resource(&self, offset: u64, data: &Self::ResourceData) -> Result<Self::Resource> {
        self.download(&data.0, &data.1, offset).await
    }

    fn advise<'a>(
        &self,
        state: &'a QueueState,
        _data: &mut Self::AdviseData,
    ) -> Result<(Duration, Option<Action<'a>>)> {
        self.download_strategy.advise(state)
    }
}

pub(crate) struct ObjectReader {
    bucket: Option<String>,
    path: String,
    offset: u64,
    size: u64,
    error_count: usize,
    stream: Box<dyn ReadStream + Send + Unpin>,
    _download_permit: OwnedSemaphorePermit,
}

impl AsyncRead for ObjectReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.stream).poll_read(cx, buf);
        if let Poll::Ready(res) = &result {
            match res {
                Ok(bytes_read) => {
                    self.offset += *bytes_read as u64;
                }
                Err(_) => {
                    self.error_count += 1;
                }
            }
        }
        result
    }
}

impl AsyncSeek for ObjectReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        let result = Pin::new(&mut self.stream).poll_seek(cx, pos);
        if let Poll::Ready(res) = &result {
            match res {
                Ok(position) => {
                    self.offset = *position;
                }
                Err(_) => {
                    self.error_count += 1;
                }
            }
        }
        result
    }
}

impl Resource for ObjectReader {
    fn offset(&self) -> u64 {
        self.offset
    }

    fn can_reuse(&self) -> bool {
        self.offset < self.size && self.error_count == 0
    }

    async fn finalize(self) -> Result<()> {
        Ok(())
    }
}

impl Drop for ObjectReader {
    #[instrument(skip(self), name = "object_reader_drop")]
    fn drop(&mut self) {
        tracing::debug!(
            bucket = self.bucket,
            path = self.path,
            offset = self.offset,
            "object_reader closed"
        );
    }
}
