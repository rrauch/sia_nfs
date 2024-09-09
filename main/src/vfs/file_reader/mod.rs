mod renterd_download;

use crate::io_scheduler::{ResourceManager, Scheduler};
use crate::vfs::file_reader::renterd_download::RenterdDownload;
use crate::vfs::locking::ReadLock;
use bytes::{Buf, Bytes};
use cachalot::Cachalot;
use futures::{ready, AsyncRead, AsyncSeek};
use futures_util::AsyncReadExt;
use renterd_client::Client;
use std::cmp::min;
use std::future::Future;
use std::io::{Cursor, ErrorKind, SeekFrom};
use std::num::NonZeroUsize;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::task::JoinHandle;

pub(crate) struct FileReaderManager {
    scheduler: Arc<Scheduler<RenterdDownload>>,
    cachalot: Arc<Cachalot>,
}

impl FileReaderManager {
    pub fn new(
        renterd: Client,
        max_concurrent_downloads: NonZeroUsize,
        cachalot: Cachalot,
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

        Ok(Self {
            scheduler: Arc::new(scheduler),
            cachalot: Arc::new(cachalot),
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
            self.cachalot.clone(),
            size,
            read_lock,
        )
        .await
    }
}

pub struct FileReader {
    scheduler: Arc<Scheduler<RenterdDownload>>,
    cachalot: Arc<Cachalot>,
    access_key: <RenterdDownload as ResourceManager>::AccessKey,
    offset: u64,
    size: u64,
    chunk_size: usize,
    current_data: Option<(u64, Cursor<Bytes>)>,
    pending: Option<(u64, JoinHandle<anyhow::Result<Bytes>>)>,
    _read_lock: ReadLock,
}

impl FileReader {
    async fn new(
        bucket: String,
        path: String,
        scheduler: Arc<Scheduler<RenterdDownload>>,
        cachalot: Arc<Cachalot>,
        size: u64,
        read_lock: ReadLock,
    ) -> anyhow::Result<Self> {
        let prepare_key = (bucket, path);
        let access_key = scheduler.prepare(&prepare_key).await?;
        let chunk_size = cachalot.chunk_size();
        Ok(Self {
            scheduler,
            access_key,
            cachalot,
            offset: 0,
            size,
            chunk_size,
            current_data: None,
            pending: None,
            _read_lock: read_lock,
        })
    }

    fn prepare_data(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        required_chunk: u64,
        relative_offset: usize,
    ) -> Poll<std::io::Result<()>> {
        if let Some((chunk, _)) = self.pending.as_ref() {
            if *chunk != required_chunk {
                // drop this pending task
                let (_, handle) = self.pending.take().unwrap();
                handle.abort();
            }
        }

        if self.pending.is_none() {
            let handle = tokio::spawn(get_bytes(
                self.scheduler.clone(),
                self.cachalot.clone(),
                self.access_key.clone(),
                required_chunk,
            ));
            self.pending = Some((required_chunk, handle));
        }

        let (_, handle) = self.pending.as_mut().unwrap();
        let res = ready!(pin!(handle).as_mut().poll(cx));
        self.pending.take();

        let mut cursor = match res.map_err(|e| e.into()) {
            Ok(Ok(bytes)) => Cursor::new(bytes),
            Err(err) | Ok(Err(err)) => {
                tracing::error!(error = %err, "error preparing data");
                return Poll::Ready(Err(std::io::Error::from(ErrorKind::Other)));
            }
        };

        if cursor.get_ref().len() <= relative_offset {
            return Poll::Ready(Err(std::io::Error::from(ErrorKind::UnexpectedEof)));
        }

        cursor.set_position(relative_offset as u64);
        self.current_data = Some((required_chunk, cursor));
        self.warmup_next_chunk(required_chunk);

        Poll::Ready(Ok(()))
    }

    fn warmup_next_chunk(mut self: Pin<&mut Self>, current_chunk: u64) {
        let num_chunks = self.size.div_ceil(self.chunk_size as u64);
        if current_chunk >= num_chunks - 1 {
            // current_chunk is the last chunk already
            return;
        }

        let next_chunk = current_chunk + 1;
        if let Some((pending_chunk, handle)) = &self.pending {
            if *pending_chunk != next_chunk {
                handle.abort();
                self.pending.take();
            }
        }

        if self.pending.is_none() {
            self.pending = Some((
                next_chunk,
                tokio::spawn(get_bytes(
                    self.scheduler.clone(),
                    self.cachalot.clone(),
                    self.access_key.clone(),
                    next_chunk,
                )),
            ))
        }
    }

    pub fn eof(&self) -> bool {
        self.offset >= self.size
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

async fn get_bytes(
    scheduler: Arc<Scheduler<RenterdDownload>>,
    cachalot: Arc<Cachalot>,
    access_key: <RenterdDownload as ResourceManager>::AccessKey,
    chunk: u64,
) -> anyhow::Result<Bytes> {
    let (bucket, path, version) = &access_key;
    let chunk_size = cachalot.chunk_size();
    let offset = chunk_size as u64 * chunk;

    Ok(cachalot
        .try_get_with(
            bucket,
            path,
            version.as_u64(),
            chunk,
            get_from_renterd(scheduler, access_key.clone(), offset, chunk_size),
        )
        .await?)
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

fn calculate_chunk_and_offset(offset: u64, chunk_size: usize) -> (u64, usize) {
    let chunk_size = chunk_size as u64;
    let chunk_nr = offset / chunk_size;
    let relative_offset = (offset % chunk_size) as usize;
    (chunk_nr, relative_offset)
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
            let (required_chunk, relative_offset) =
                calculate_chunk_and_offset(self.offset, self.chunk_size);

            ready!(self
                .as_mut()
                .prepare_data(cx, required_chunk, relative_offset))?;
        }

        let (_, cursor) = match self.current_data.as_mut() {
            Some((current_chunk, cursor)) if cursor.has_remaining() => (*current_chunk, cursor),
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

        let (required_chunk, relative_offset) = calculate_chunk_and_offset(offset, self.chunk_size);
        if let Some((current_chunk, cursor)) = self.current_data.as_mut() {
            if current_chunk == &required_chunk {
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
            .prepare_data(cx, required_chunk, relative_offset))?;
        self.offset = offset;
        Poll::Ready(Ok(offset))
    }
}
