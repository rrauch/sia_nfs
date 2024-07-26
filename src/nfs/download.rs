use crate::vfs::{File, FileReader, Vfs};
use anyhow::{anyhow, bail, Result};
use futures::{AsyncRead, AsyncSeek};
use futures_util::AsyncSeekExt;
use std::io;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::instrument;

pub(crate) struct DownloadManager {
    idle: Arc<Mutex<Vec<(DownloadInner, SystemTime)>>>,
    notify_reaper: Arc<Notify>,
    max_idle: Duration,
    vfs: Arc<Vfs>,
    download_limiter: Arc<Semaphore>,
    reaper: JoinHandle<()>,
}

impl Drop for DownloadManager {
    fn drop(&mut self) {
        self.reaper.abort();
    }
}

impl DownloadManager {
    pub fn new(vfs: Arc<Vfs>, max_downloads: usize, max_idle: Duration) -> Self {
        let download_limiter = Arc::new(Semaphore::new(max_downloads));
        let idle = Arc::new(Mutex::new(Vec::with_capacity(max_downloads)));
        let notify_reaper = Arc::new(Notify::new());

        let reaper = {
            let notify = notify_reaper.clone();
            let idle = idle.clone();
            tokio::spawn(async move {
                loop {
                    let mut next_check = SystemTime::now() + Duration::from_secs(60);
                    {
                        let mut downloads = idle.lock().unwrap();
                        let now = SystemTime::now();
                        downloads.retain(|(_, expiration)| expiration > &now);

                        for (_, expiration) in downloads.iter() {
                            if expiration < &next_check {
                                next_check = *expiration;
                            }
                        }
                    }

                    let sleep_duration = next_check
                        .duration_since(SystemTime::now())
                        .unwrap_or(Duration::from_secs(1));

                    tokio::select! {
                        _ = tokio::time::sleep(sleep_duration) => {},
                        _ = notify.notified() => {}
                    }
                }
            })
        };
        Self {
            idle,
            notify_reaper,
            max_idle,
            vfs,
            download_limiter,
            reaper,
        }
    }

    pub async fn download(&self, file: &File, offset: u64) -> Result<Download> {
        if offset > file.size() {
            bail!("offset beyond eof");
        }

        tracing::trace!("waiting for download permit");
        let permit = timeout(
            Duration::from_secs(60),
            self.download_limiter.clone().acquire_owned(),
        )
        .await??;

        // Try to find an existing download first
        let dl = {
            let mut lock = self.idle.lock().unwrap();
            Self::find_download(file, offset, &mut lock)
        };

        let mut dl = match dl {
            Some(dl) => {
                tracing::debug!(
                    id = file.id(),
                    name = file.name(),
                    offset = dl.file_reader.offset(),
                    "re-using download"
                );
                dl
            }
            None => {
                // no suitable download found, initiate a new one
                tracing::debug!(
                    id = file.id(),
                    name = file.name(),
                    offset,
                    "initiating new download"
                );

                let file_reader = self.vfs.read_file(file).await?;
                DownloadInner { file_reader }
            }
        };

        let pos = dl.file_reader.seek(SeekFrom::Start(offset)).await?;
        if pos != offset {
            return Err(anyhow!("seeking failed"));
        }

        Ok(Download {
            inner: Some(dl),
            _permit: permit,
            pool: self.idle.clone(),
            notify_reaper: self.notify_reaper.clone(),
            max_idle: self.max_idle.clone(),
            io_error: false,
        })
    }

    fn find_download(
        file: &File,
        offset: u64,
        downloads: &mut Vec<(DownloadInner, SystemTime)>,
    ) -> Option<DownloadInner> {
        // remove expired downloads first
        let now = SystemTime::now();
        downloads.retain(|(_, expiration)| expiration > &now);

        // find the "closest" (distance to offset) download for the current file
        if let Some(index) = downloads
            .iter()
            .enumerate()
            .filter(|(_, (dl, _))| dl.file_reader.file() == file)
            .min_by_key(|(_, (dl, _))| {
                let current_offset = dl.file_reader.offset();
                if current_offset > offset {
                    current_offset - offset
                } else {
                    offset - current_offset
                }
            })
            .map(|(index, _)| index)
        {
            Some(downloads.remove(index).0)
        } else {
            None
        }
    }
}

pub(crate) struct Download {
    inner: Option<DownloadInner>,
    _permit: OwnedSemaphorePermit,
    pool: Arc<Mutex<Vec<(DownloadInner, SystemTime)>>>,
    max_idle: Duration,
    notify_reaper: Arc<Notify>,
    io_error: bool,
}

impl Download {
    pub fn eof(&self) -> bool {
        match &self.inner {
            None => true,
            Some(inner) => inner.file_reader.eof(),
        }
    }

    pub fn offset(&self) -> u64 {
        self.inner.as_ref().unwrap().file_reader.offset()
    }
}

impl Drop for Download {
    #[instrument(skip(self))]
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // only return it if there was no io error and eof is not yet reached
            if !self.io_error && !inner.file_reader.eof() {
                {
                    let mut pool = self.pool.lock().unwrap();
                    let expires = SystemTime::now() + self.max_idle;
                    pool.push((inner, expires));
                }
                self.notify_reaper.notify_one();
                tracing::debug!("download returned to pool");
            }
        }
    }
}

impl AsyncRead for Download {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let inner = match self.inner.as_mut() {
            Some(inner) => inner,
            None => return Poll::Ready(Ok(0)),
        };

        let result = Pin::new(&mut inner.file_reader).poll_read(cx, buf);
        if let Poll::Ready(Err(_)) = &result {
            self.io_error = true;
        }
        result
    }
}

impl AsyncSeek for Download {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let inner = match self.inner.as_mut() {
            Some(inner) => inner,
            None => return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))),
        };

        let result = Pin::new(&mut inner.file_reader).poll_seek(cx, pos);
        if let Poll::Ready(Err(_)) = &result {
            self.io_error = true;
        }
        result
    }
}

struct DownloadInner {
    file_reader: FileReader,
}

impl Drop for DownloadInner {
    #[instrument(skip(self), name = "download_drop")]
    fn drop(&mut self) {
        tracing::debug!(
            id = self.file_reader.file().id(),
            name = self.file_reader.file().name(),
            offset = self.file_reader.offset(),
            "download closed"
        );
    }
}
