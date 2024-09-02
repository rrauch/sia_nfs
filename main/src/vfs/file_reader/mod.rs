mod renterd_download;

use crate::io_scheduler::{ResourceManager, Scheduler};
use crate::vfs::file_reader::renterd_download::RenterdDownload;
use crate::vfs::locking::ReadLock;
use bytes::{Buf, Bytes};
use cachalot::Cachalot;
use futures::{ready, AsyncRead, AsyncSeek};
use futures_util::future::BoxFuture;
use futures_util::{AsyncReadExt, FutureExt};
use moka::future::{Cache, CacheBuilder};
use renterd_client::Client;
use std::cmp::min;
use std::io::{Cursor, ErrorKind, SeekFrom};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

pub(crate) struct FileReaderManager {
    scheduler: Arc<Scheduler<RenterdDownload>>,
    page_size: usize,
    l1_cache: Cache<(<RenterdDownload as ResourceManager>::AccessKey, u64, usize), Bytes>,
    cachalot: Option<Arc<Cachalot>>,
}

impl FileReaderManager {
    pub fn new(
        renterd: Client,
        max_concurrent_downloads: NonZeroUsize,
        page_size: usize,
        l1_cache_size: u64,
        cachalot: Option<Cachalot>,
    ) -> anyhow::Result<Self> {
        let scheduler = RenterdDownload::new(
            renterd,
            max_concurrent_downloads,
            Duration::from_secs(1),
            Duration::from_millis(600),
            NonZeroUsize::new(5).unwrap(),
            Duration::from_millis(1000),
            Duration::from_millis(250),
        );

        let l1_cache = CacheBuilder::new(l1_cache_size)
            .weigher(|_, v: &Bytes| v.len() as u32)
            .build();

        Ok(Self {
            scheduler: Arc::new(scheduler),
            page_size,
            l1_cache,
            cachalot: cachalot.map(|c| Arc::new(c)),
        })
    }

    pub async fn open_reader(
        &self,
        bucket: String,
        path: String,
        size: u64,
        read_lock: ReadLock,
    ) -> anyhow::Result<FileReader> {
        FileReader::new(
            bucket,
            path,
            self.scheduler.clone(),
            self.l1_cache.clone(),
            self.cachalot.clone(),
            size,
            self.page_size,
            read_lock,
        )
        .await
    }
}

pub struct FileReader {
    scheduler: Arc<Scheduler<RenterdDownload>>,
    cache: Cache<(<RenterdDownload as ResourceManager>::AccessKey, u64, usize), Bytes>,
    cachalot: Option<Arc<Cachalot>>,
    access_key: <RenterdDownload as ResourceManager>::AccessKey,
    offset: u64,
    size: u64,
    page_size: usize,
    current_data: Option<(u64, Cursor<Bytes>)>,
    pending: Option<(u64, BoxFuture<'static, anyhow::Result<Bytes>>)>,
    _read_lock: ReadLock,
}

impl FileReader {
    async fn new(
        bucket: String,
        path: String,
        scheduler: Arc<Scheduler<RenterdDownload>>,
        cache: Cache<(<RenterdDownload as ResourceManager>::AccessKey, u64, usize), Bytes>,
        cachalot: Option<Arc<Cachalot>>,
        size: u64,
        page_size: usize,
        read_lock: ReadLock,
    ) -> anyhow::Result<Self> {
        let prepare_key = (bucket, path);
        let access_key = scheduler.prepare(&prepare_key).await?;
        Ok(Self {
            scheduler,
            access_key,
            cache,
            cachalot,
            offset: 0,
            size,
            page_size,
            current_data: None,
            pending: None,
            _read_lock: read_lock,
        })
    }

    fn prepare_data(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        required_page: u64,
        relative_offset: usize,
    ) -> Poll<std::io::Result<()>> {
        if let Some((page, _)) = self.pending.as_ref() {
            if *page != required_page {
                // drop this pending future
                self.pending.take();
            }
        }

        if self.pending.is_none() {
            let fut = get_bytes(
                self.cache.clone(),
                self.scheduler.clone(),
                self.cachalot.clone(),
                self.access_key.clone(),
                required_page,
                required_page * self.page_size as u64,
                self.page_size,
            )
            .boxed();
            self.pending = Some((required_page, fut));
        }

        let (_, fut) = self.pending.as_mut().unwrap();
        let res = ready!(fut.as_mut().poll(cx));
        self.pending.take();

        let mut cursor = match res {
            Ok(bytes) => Cursor::new(bytes),
            Err(err) => {
                tracing::error!(error = %err, "error preparing data");
                return Poll::Ready(Err(std::io::Error::from(ErrorKind::Other)))
            },
        };

        if cursor.get_ref().len() <= relative_offset {
            return Poll::Ready(Err(std::io::Error::from(ErrorKind::UnexpectedEof)));
        }

        cursor.set_position(relative_offset as u64);
        self.current_data = Some((required_page, cursor));
        Poll::Ready(Ok(()))
    }

    pub fn eof(&self) -> bool {
        self.offset >= self.size
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

async fn get_bytes(
    cache: Cache<(<RenterdDownload as ResourceManager>::AccessKey, u64, usize), Bytes>,
    scheduler: Arc<Scheduler<RenterdDownload>>,
    cachalot: Option<Arc<Cachalot>>,
    access_key: <RenterdDownload as ResourceManager>::AccessKey,
    page: u64,
    offset: u64,
    size: usize,
) -> anyhow::Result<Bytes> {
    let cache_key = (access_key.clone(), offset, size);
    cache
        .try_get_with(cache_key, async {
            let (bucket, path, version) = &access_key;
            let version: Vec<u8> = version.into();

            if let Some(cachalot) = cachalot.as_ref() {
                // first, try cachalot
                if let Some(content) = cachalot.get(bucket, path, version.as_ref(), page).await? {
                    return Ok(content);
                }
            }
            // try to get from renterd
            let content = get_from_renterd(scheduler, access_key.clone(), offset, size).await?;

            if let Some(cachalot) = cachalot.as_ref() {
                cachalot
                    .put(bucket, path, version.as_ref(), page, content.clone())
                    .await?;
            }

            Ok(content)
        })
        .await
        .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!(e))
}

async fn get_from_renterd(
    scheduler: Arc<Scheduler<RenterdDownload>>,
    access_key: <RenterdDownload as ResourceManager>::AccessKey,
    offset: u64,
    size: usize,
) -> anyhow::Result<Bytes> {
    let mut reader = scheduler.access(&access_key, offset).await?;
    let reader = reader.as_mut();
    let mut buffer = vec![0u8; size];
    let mut total_read = 0;

    while total_read < size {
        match reader.read(&mut buffer[total_read..]).await? {
            0 => break, // EOF reached
            n => total_read += n,
        }
    }

    if total_read == 0 {
        return Err(std::io::Error::from(ErrorKind::UnexpectedEof).into());
    }

    buffer.truncate(total_read);

    Ok(Bytes::from(buffer))
}

fn calculate_page_and_offset(offset: u64, page_size: usize) -> (u64, usize) {
    let page_size = page_size as u64;
    let page_nr = offset / page_size;
    let relative_offset = (offset % page_size) as usize;
    (page_nr, relative_offset)
}

impl AsyncRead for FileReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.offset >= self.size {
            // eof
            return Poll::Ready(Ok(0));
        }

        if match self.current_data.as_ref() {
            Some((_, cursor)) => !cursor.has_remaining(),
            _ => true,
        } {
            self.current_data.take();
        }

        if self.current_data.is_none() {
            let (required_page, relative_offset) =
                calculate_page_and_offset(self.offset, self.page_size);

            ready!(self
                .as_mut()
                .prepare_data(cx, required_page, relative_offset))?;
        }

        let cursor = match self.current_data.as_mut() {
            Some((_, cursor)) if cursor.has_remaining() => cursor,
            _ => {
                return Poll::Ready(Err(std::io::Error::from(ErrorKind::UnexpectedEof)));
            }
        };

        let num_bytes = min(buf.len(), cursor.remaining());
        cursor.copy_to_slice(&mut buf[..num_bytes]);
        self.offset += num_bytes as u64;
        Poll::Ready(Ok(num_bytes))
    }
}

impl AsyncSeek for FileReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        let offset = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(diff) => (self.size as i64 + diff) as u64,
            SeekFrom::Current(diff) => (self.offset as i64 + diff) as u64,
        };

        if offset >= self.size {
            return Poll::Ready(Err(std::io::Error::from(ErrorKind::InvalidInput)));
        }

        let (required_page, relative_offset) = calculate_page_and_offset(offset, self.page_size);
        if let Some((current_page, cursor)) = self.current_data.as_mut() {
            if current_page == &required_page {
                let size = cursor.get_ref().len();
                if relative_offset > size {
                    return Poll::Ready(Err(std::io::Error::from(ErrorKind::InvalidInput)));
                }
                cursor.set_position(relative_offset as u64);
                self.offset = offset;
                return Poll::Ready(Ok(offset));
            }
        }

        self.current_data.take();
        ready!(self
            .as_mut()
            .prepare_data(cx, required_page, relative_offset))?;
        self.offset = offset;
        Poll::Ready(Ok(offset))
    }
}
