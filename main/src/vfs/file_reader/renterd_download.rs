use crate::io_scheduler::resource_manager::Action::{Again, Sleep};
use crate::io_scheduler::resource_manager::{
    Action, Context, Entry, QueueCtrl, Resource, ResourceManager,
};
use crate::io_scheduler::Scheduler;
use anyhow::Result;
use anyhow::{anyhow, bail};
use bytes::Bytes;
use cachalot::Cachalot;
use futures::AsyncRead;
use futures_util::future::BoxFuture;
use futures_util::{AsyncReadExt, FutureExt};
use itertools::Itertools;
use renterd_client::worker::object::DownloadableObject;
use renterd_client::Client;
use std::cmp::min;
use std::hash::Hasher;
use std::num::{NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, SystemTime};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tracing::instrument;

const VERSION_HASH_SEED: u64 = 3986469829483842332;

pub(super) struct RenterdDownload {
    renterd: Client,
    cachalot: Arc<Cachalot>,
    max_skip_ahead: u64,
    download_limiter: Arc<Semaphore>,
    max_active: usize,
    idle_min_wait: Duration,
    idle_timeout_min: Duration,
    wait_min_advance: Duration,
    wait_min_new: Duration,
}

impl RenterdDownload {
    pub(super) fn new(
        renterd: Client,
        cachalot: Arc<Cachalot>,
        max_skip_ahead: NonZeroU64,
        max_concurrent_downloads: NonZeroUsize,
        max_queue_idle: Duration,
        idle_min_wait: Duration,
        idle_timeout_min: Duration,
        idle_timeout_max: Duration,
        wait_min_advance: Duration,
        wait_min_new: Duration,
    ) -> Scheduler<Self> {
        let max_active = max_concurrent_downloads.get();
        let download_limiter = Arc::new(Semaphore::new(max_active));

        let max_skip_ahead = {
            // round up to nearest multiple of chunk size
            let chunk_size = cachalot.chunk_size() as u64;
            let max_skip_ahead = max_skip_ahead.get();
            if max_skip_ahead % chunk_size == 0 {
                max_skip_ahead
            } else {
                ((max_skip_ahead / chunk_size) + 1) * chunk_size
            }
        };

        let renterd_download = Self {
            renterd,
            cachalot,
            max_skip_ahead,
            download_limiter,
            max_active,
            idle_min_wait,
            idle_timeout_min,
            wait_min_advance,
            wait_min_new,
        };

        Scheduler::new(renterd_download, true, max_queue_idle, idle_timeout_max, 2)
    }

    async fn advance(
        mut reader: ObjectReader,
        dst_offset: u64,
        cachalot: Arc<Cachalot>,
        dl: &DownloadableObject,
        version: &Version,
        max_skip_ahead: u64,
    ) -> Result<ObjectReader> {
        if dst_offset >= reader.size {
            bail!("advance beyond reader size");
        }

        if reader.offset >= dst_offset {
            bail!("dst_offset needs to be ahead of current reader.offset");
        }

        let n = dst_offset.saturating_sub(reader.offset);
        if n > max_skip_ahead {
            bail!("advance beyond MAX_SKIP_AHEAD");
        }

        tracing::debug!(
            begin_offset = reader.offset,
            end_offset = dst_offset,
            num_bytes = n,
            "advancing reader"
        );

        let bucket = dl.bucket.as_ref().map(|s| s.as_str()).unwrap_or_default();
        let path = dl.path.as_str();
        let version = version.as_u64();

        let chunk_size = cachalot.chunk_size() as u64;

        let next_chunk_in = if reader.offset % chunk_size == 0 {
            0
        } else {
            chunk_size - (reader.offset % chunk_size)
        };

        if next_chunk_in > 0 {
            // only partial chunk, skipping
            tracing::trace!(bytes_to_skip = next_chunk_in, "skipping bytes");
            let mut take = reader.take(next_chunk_in);
            futures::io::copy(&mut take, &mut futures::io::sink()).await?;
            reader = take.into_inner();
        }

        while reader.offset < dst_offset {
            let size = min(dst_offset - reader.offset, chunk_size) as usize;
            let chunk_nr = reader.offset / chunk_size;
            let expected_offset = reader.offset + size as u64;

            if size == (chunk_size as usize) || reader.offset + size as u64 >= reader.size {
                // making sure this is a full chunk
                // the final chunk of the file may be smaller
                cachalot
                    .try_get_with(bucket, path, version, chunk_nr, async {
                        let mut buffer = vec![0u8; size];
                        reader.read_exact(&mut buffer).await?;
                        Ok(Bytes::from(buffer))
                    })
                    .await?;
            }

            if reader.offset < expected_offset {
                let bytes_to_skip = expected_offset - reader.offset;
                tracing::trace!(bytes_to_skip, "skipping bytes");
                let mut take = reader.take(bytes_to_skip);
                futures::io::copy(&mut take, &mut futures::io::sink()).await?;
                reader = take.into_inner();
            }
        }

        Ok(reader)
    }

    async fn download(
        renterd: &Client,
        download_limiter: Arc<Semaphore>,
        known_dl: &DownloadableObject,
        known_version: &Version,
        offset: u64,
    ) -> Result<ObjectReader> {
        tracing::debug!(offset, "starting new download");

        tracing::trace!("waiting for download permit");
        let download_permit =
            timeout(Duration::from_secs(60), download_limiter.acquire_owned()).await??;
        tracing::trace!("download permit acquired");

        let (new_dl, new_version) = Self::dl_object(
            renterd,
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

        let stream = new_dl.open_stream(offset).await?;

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

    async fn dl_object(
        renterd: &Client,
        bucket: String,
        path: &str,
    ) -> Result<(DownloadableObject, Version)> {
        let dl_object = renterd
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
pub struct Version(u64);

impl Version {
    pub fn as_u64(&self) -> u64 {
        self.0
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
        Version(hasher.finish())
    }
}

impl ResourceManager for RenterdDownload {
    type Resource = ObjectReader;
    type PreparationKey = (String, String);
    type AccessKey = (String, String, Version);
    type ResourceData = Arc<(DownloadableObject, Version)>;
    type ResourceFuture = BoxFuture<'static, Result<Self::Resource>>;

    async fn prepare(
        &self,
        preparation_key: &Self::PreparationKey,
    ) -> Result<(Self::AccessKey, Self::ResourceData, Vec<Self::Resource>)> {
        let (bucket, path) = preparation_key;
        let (dl_object, version) = Self::dl_object(&self.renterd, bucket.clone(), path).await?;

        Ok((
            (bucket.clone(), path.clone(), version.clone()),
            Arc::new((dl_object, version)),
            vec![],
        ))
    }

    fn process(
        &self,
        queue: &mut QueueCtrl<Self>,
        data: &mut Self::ResourceData,
        _ctx: &Context,
    ) -> Result<Action> {
        let entries = queue.entries();
        // calculate available slots
        let mut unused_slots = self.max_active.saturating_sub(
            entries
                .iter()
                .filter(|e| e.as_active().is_some() || e.as_idle().is_some())
                .count(),
        );

        // divide entries into clusters
        let clusters = cluster_entries(entries, self.max_skip_ahead);

        //debug_print(ctx, &clusters);

        let wait_new_threshold = SystemTime::now() - self.wait_min_new;
        let wait_advance_threshold = SystemTime::now() - self.wait_min_advance;
        let idle_wait_threshold = SystemTime::now() - self.idle_min_wait;

        let mut extra_slots_needed = 0usize;
        let mut sleep = Duration::from_millis(1000);

        for cluster in clusters {
            let first = cluster
                .get(0)
                .expect("cluster should have at least one entry");

            let second = cluster.get(1);

            match first {
                Entry::Waiting(waiting) => {
                    if waiting.offset == 0 || waiting.since <= wait_new_threshold {
                        if unused_slots > 0 {
                            let data = data.clone();
                            let download_limiter = self.download_limiter.clone();
                            let renterd = self.renterd.clone();
                            let offset = waiting.offset;

                            queue.prepare(
                                waiting,
                                async move {
                                    Self::download(
                                        &renterd,
                                        download_limiter,
                                        &data.0,
                                        &data.1,
                                        offset,
                                    )
                                    .await
                                }
                                .boxed(),
                            )?;

                            unused_slots = unused_slots.saturating_sub(1);
                        } else {
                            extra_slots_needed += 1;
                        }
                    } else {
                        sleep = min(calc_wait_duration(waiting.since, wait_new_threshold), sleep);
                    }
                }
                Entry::Idle(idle) => {
                    let waiting = second.map(|e| e.as_waiting()).flatten();
                    if waiting.is_some() {
                        if idle.since <= idle_wait_threshold {
                            let waiting = waiting.unwrap();
                            if waiting.since <= wait_advance_threshold {
                                if let Some(reader) = queue.take_idle(idle) {
                                    let offset = waiting.offset;
                                    let data = data.clone();
                                    let cachalot = self.cachalot.clone();
                                    let max_skip_ahead = self.max_skip_ahead;
                                    queue.prepare(
                                        waiting,
                                        async move {
                                            Self::advance(
                                                reader,
                                                offset,
                                                cachalot,
                                                &data.0,
                                                &data.1,
                                                max_skip_ahead,
                                            )
                                            .await
                                        }
                                        .boxed(),
                                    )?;
                                }
                            } else {
                                // still have to wait a little longer
                                sleep = min(
                                    calc_wait_duration(waiting.since, wait_advance_threshold),
                                    sleep,
                                );
                            }
                        } else {
                            sleep = min(calc_wait_duration(idle.since, idle_wait_threshold), sleep);
                        }
                    }
                }
                Entry::Active(_) => {
                    // do nothing here
                }
            }
        }

        if extra_slots_needed > 0 {
            // a previous task could not be started because max active had been reached
            // under pressure we can free idle tasks early
            // idle_timeout_min has to be reached to be freeable
            let idle_timeout_threshold = SystemTime::now() - self.idle_timeout_min;

            tracing::trace!(extra_slots_needed, "attempting to free extra slots");

            // get fresh entries
            let clusters = cluster_entries(queue.entries(), self.max_skip_ahead);

            let mut next_expiration = None;
            let mut again = false;

            for idle in clusters
                .into_iter()
                .filter_map(|v| {
                    // only consider clusters with single idle entries
                    if v.len() == 1 {
                        match v.into_iter().next() {
                            Some(Entry::Idle(idle)) => {
                                // make sure they have exceeded the soft idle timeout
                                if idle.since <= idle_timeout_threshold {
                                    Some(idle)
                                } else {
                                    // not yet, update next_expiration if applicable
                                    let expires_at = idle.since + self.idle_timeout_min;
                                    next_expiration = Some(match next_expiration {
                                        None => expires_at,
                                        Some(current) => min(current, expires_at),
                                    });
                                    None
                                }
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                })
                .sorted_unstable_by(|a, b| a.since.cmp(&b.since))
                .take(extra_slots_needed)
            {
                tracing::debug!(offset = idle.offset, "freeing idle task early");
                queue.finalize(&idle)?;
                again = true;
            }

            if again {
                // we were able to free extra resources, run again
                return Ok(Again);
            }

            if let Some(next_expiration) = next_expiration {
                sleep = min(
                    calc_wait_duration(next_expiration, SystemTime::now()),
                    sleep,
                );
            }
        }

        Ok(Sleep(sleep))
    }
}

/*fn debug_print(ctx: &Context, clusters: &Vec<Vec<Entry>>) {
    let since_start = SystemTime::now().duration_since(ctx.started).unwrap();
    let since_last_run = SystemTime::now()
        .duration_since(ctx.previous_call.unwrap_or_else(|| SystemTime::now()))
        .unwrap_or_default();
    let iteration = ctx.iteration;

    println!(
        "============================================ {} / {} ms  @ {} ms",
        iteration,
        since_last_run.as_millis(),
        since_start.as_millis()
    );
    println!("------ found {} clusters", clusters.len());
    for (i, cluster) in clusters.iter().enumerate() {
        let active = cluster.iter().filter_map(|e| e.as_active()).collect_vec();
        let idle = cluster.iter().filter_map(|e| e.as_idle()).collect_vec();
        let waiting = cluster.iter().filter_map(|e| e.as_waiting()).collect_vec();

        println!(
            "--- cluster {}: {}-{} ({}), active: {}, idle:{}, waiting: {}",
            i,
            cluster.first().unwrap().offset(),
            cluster.last().unwrap().offset(),
            cluster.len(),
            active.len(),
            idle.len(),
            waiting.len()
        );

        println!("              active:");
        for e in &active {
            println!(
                "                      {} / {} ms",
                e.offset,
                SystemTime::now()
                    .duration_since(e.since)
                    .unwrap()
                    .as_millis()
            );
        }

        println!("              idle:");
        for e in &idle {
            println!(
                "                      {} / {} ms",
                e.offset,
                SystemTime::now()
                    .duration_since(e.since)
                    .unwrap()
                    .as_millis()
            );
        }

        println!("              waiting:");
        for e in &waiting {
            println!(
                "                      {} / {} ms",
                e.offset,
                SystemTime::now()
                    .duration_since(e.since)
                    .unwrap()
                    .as_millis()
            );
        }
    }
}*/

fn cluster_entries(entries: Vec<Entry>, max_distance: u64) -> Vec<Vec<Entry>> {
    if entries.is_empty() {
        return vec![];
    }

    let mut clusters: Vec<Vec<Entry>> = Vec::new();
    let mut current_cluster: Vec<Entry> = Vec::new();

    for entry in entries {
        if current_cluster.is_empty()
            || entry.offset() - current_cluster.last().unwrap().offset() <= max_distance
                && entry.as_waiting().is_some()
        {
            current_cluster.push(entry);
        } else {
            clusters.push(current_cluster);
            current_cluster = vec![entry];
        }
    }

    if !current_cluster.is_empty() {
        clusters.push(current_cluster);
    }

    clusters
}

fn calc_wait_duration(target: SystemTime, earlier: SystemTime) -> Duration {
    target.duration_since(earlier).unwrap_or_default()
}

pub(crate) struct ObjectReader {
    bucket: Option<String>,
    path: String,
    offset: u64,
    size: u64,
    error_count: usize,
    stream: Box<dyn AsyncRead + Send + Unpin>,
    _download_permit: OwnedSemaphorePermit,
}

impl AsyncRead for ObjectReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
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
