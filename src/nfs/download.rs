use crate::vfs::{File, FileReader, Vfs};
use anyhow::{anyhow, bail, Result};
use futures::{AsyncRead, AsyncSeek};
use futures_util::AsyncSeekExt;
use std::collections::HashMap;
use std::io;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::instrument;

pub(crate) struct DownloadManager {
    idle: Arc<Mutex<Vec<(FileReader, SystemTime)>>>,
    download_limiter: Arc<Mutex<HashMap<u64, Arc<Semaphore>>>>,
    notify_reaper: Arc<Notify>,
    max_downloads_per_file: usize,
    max_idle: Duration,
    vfs: Arc<Vfs>,
    reaper: JoinHandle<()>,
}

impl Drop for DownloadManager {
    fn drop(&mut self) {
        self.reaper.abort();
    }
}

impl DownloadManager {
    pub fn new(vfs: Arc<Vfs>, max_downloads_per_file: NonZeroUsize, max_idle: Duration) -> Self {
        let max_downloads_per_file = max_downloads_per_file.get();
        let idle = Arc::new(Mutex::new(vec![]));
        let download_limiter = Arc::new(Mutex::new(HashMap::default()));
        let notify_reaper = Arc::new(Notify::new());

        let reaper = {
            let notify = notify_reaper.clone();
            let idle = idle.clone();
            let download_limiter = download_limiter.clone();
            tokio::spawn(async move {
                loop {
                    let mut next_check = SystemTime::now() + Duration::from_secs(60);
                    {
                        // remove expired downloads
                        {
                            let mut idle = idle.lock().unwrap();
                            let now = SystemTime::now();
                            idle.retain(|(_, expiration)| expiration > &now);

                            for (_, expiration) in idle.iter() {
                                if expiration < &next_check {
                                    next_check = *expiration;
                                }
                            }
                        }

                        // clean-up unused download permits
                        {
                            let mut download_limiter = download_limiter.lock().unwrap();
                            download_limiter.retain(|_, download_limiter: &mut Arc<Semaphore>| {
                                download_limiter.available_permits() < max_downloads_per_file
                            });
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
            download_limiter,
            notify_reaper,
            max_idle,
            max_downloads_per_file,
            vfs,
            reaper,
        }
    }

    pub async fn download(&self, file: &File, offset: u64) -> Result<Download> {
        if offset > file.size() {
            bail!("offset beyond eof");
        }

        let download_limiter = {
            let mut lock = self.download_limiter.lock().unwrap();
            lock.entry(file.id())
                .or_insert_with(|| Arc::new(Semaphore::new(self.max_downloads_per_file)))
                .clone()
        };
        tracing::trace!("waiting for download permit");
        let _permit = timeout(Duration::from_secs(60), download_limiter.acquire_owned()).await??;

        // Try to find an existing download first
        let file_reader = {
            let mut lock = self.idle.lock().unwrap();
            Self::find_download(file, offset, &mut lock)
        };

        let mut file_reader = match file_reader {
            Some(file_reader) => {
                tracing::debug!(
                    id = file.id(),
                    name = file.name(),
                    offset = file_reader.offset(),
                    "re-using download"
                );
                file_reader
            }
            None => {
                // no suitable download found, initiate a new one
                tracing::debug!(
                    id = file.id(),
                    name = file.name(),
                    offset,
                    "initiating new download"
                );

                self.vfs.read_file(file).await?
            }
        };

        let pos = file_reader.seek(SeekFrom::Start(offset)).await?;
        if pos != offset {
            return Err(anyhow!("seeking failed"));
        }

        Ok(Download {
            file_reader: Some(file_reader),
            _permit,
            pool: self.idle.clone(),
            notify_reaper: self.notify_reaper.clone(),
            max_idle: self.max_idle.clone(),
            io_error: false,
        })
    }

    fn find_download(
        file: &File,
        offset: u64,
        idle_downloads: &mut Vec<(FileReader, SystemTime)>,
    ) -> Option<FileReader> {
        // remove expired downloads first
        let now = SystemTime::now();
        idle_downloads.retain(|(_, expiration)| expiration > &now);

        // find the "closest" (distance to offset) download for the current file
        if let Some(index) = idle_downloads
            .iter()
            .enumerate()
            .filter(|(_, (file_reader, _))| file_reader.file() == file)
            .min_by_key(|(_, (file_reader, _))| {
                let current_offset = file_reader.offset();
                if current_offset > offset {
                    current_offset - offset
                } else {
                    offset - current_offset
                }
            })
            .map(|(index, _)| index)
        {
            Some(idle_downloads.remove(index).0)
        } else {
            None
        }
    }
}

pub(crate) struct Download {
    file_reader: Option<FileReader>,
    _permit: OwnedSemaphorePermit,
    pool: Arc<Mutex<Vec<(FileReader, SystemTime)>>>,
    max_idle: Duration,
    notify_reaper: Arc<Notify>,
    io_error: bool,
}

impl Download {
    pub fn eof(&self) -> bool {
        match &self.file_reader {
            None => true,
            Some(file_reader) => file_reader.eof(),
        }
    }
}

impl Drop for Download {
    #[instrument(skip(self))]
    fn drop(&mut self) {
        if let Some(file_reader) = self.file_reader.take() {
            // only return it if there was no io error and eof is not yet reached
            if !self.io_error && !file_reader.eof() {
                {
                    let mut pool = self.pool.lock().unwrap();
                    let expires = SystemTime::now() + self.max_idle;
                    pool.push((file_reader, expires));
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
        let file_reader = match self.file_reader.as_mut() {
            Some(file_reader) => file_reader,
            None => return Poll::Ready(Ok(0)),
        };

        let result = Pin::new(file_reader).poll_read(cx, buf);
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
        let file_reader = match self.file_reader.as_mut() {
            Some(file_reader) => file_reader,
            None => return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))),
        };

        let result = Pin::new(file_reader).poll_seek(cx, pos);
        if let Poll::Ready(Err(_)) = &result {
            self.io_error = true;
        }
        result
    }
}
