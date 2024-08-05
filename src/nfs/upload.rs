use crate::vfs::{Directory, File, FileWriter, Vfs};
use anyhow::{anyhow, bail, Result};
use arc_swap::access::DynAccess;
use arc_swap::ArcSwapOption;
use futures::AsyncWrite;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use itertools::Either;
use std::collections::HashMap;
use std::io;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

struct Entry {
    file: Arc<ArcSwapOption<File>>,
    ready_rx: watch::Receiver<bool>,
    file_writer: Option<FileWriter>,
    expiration: SystemTime,
    watch_tx: watch::Sender<u64>,
}

pub(crate) struct UploadManager {
    vfs: Arc<Vfs>,
    pending: Arc<Mutex<HashMap<(u64, String), Entry>>>,
    max_idle: Duration,
    notify_finalizer: Arc<Notify>,
    finalizer: JoinHandle<()>,
}

impl UploadManager {
    pub fn new(vfs: Arc<Vfs>, max_idle: Duration) -> Self {
        let notify_finalizer = Arc::new(Notify::new());
        let pending = Arc::new(Mutex::new(HashMap::<(u64, String), Entry>::new()));
        let finalizer = {
            let pending = pending.clone();
            let notify = notify_finalizer.clone();

            tokio::spawn(async move {
                loop {
                    let mut next_check = SystemTime::now() + Duration::from_secs(60);
                    // get all uploads ready to be finalized
                    let mut tasks = FuturesUnordered::new();
                    let keys = {
                        let mut lock = pending.lock().unwrap();
                        let now = SystemTime::now();
                        // waiting for `extract_if` to be stabilized
                        // see https://github.com/rust-lang/rust/issues/59618
                        // in the meantime doing it the manual way
                        lock.iter()
                            .filter_map(|(k, v)| {
                                if v.expiration <= now && v.file_writer.is_some() {
                                    // this entry is expired and ready for finalization
                                    return Some(k.clone());
                                }
                                None
                            })
                            .collect::<Vec<_>>()
                            .into_iter()
                            .map(|k| {
                                (
                                    k.clone(),
                                    lock.get_mut(&k).map(|e| e.file_writer.take()).flatten(),
                                )
                            })
                            .map(|(k, v)| {
                                if let Some(file_writer) = v {
                                    tasks.push(file_writer.finalize());
                                }
                                k
                            })
                            .collect::<Vec<_>>()
                    };

                    while let Some(res) = tasks.next().await {
                        match res {
                            Ok(file) => {
                                {
                                    let key = (file.parent(), file.name().to_string());
                                    let mut lock = pending.lock().unwrap();
                                    if let Some(entry) = lock.remove(&key) {
                                        if entry.file_writer.is_some() {
                                            tracing::warn!(
                                                file = ?entry.file,
                                                "removed entry has a file_writer, putting it back"
                                            );
                                            // something went wrong, putting it back
                                            lock.insert(key, entry);
                                        }
                                    } else {
                                        tracing::warn!("entry for {}/{} was missing", key.0, key.1);
                                    }
                                }
                                tracing::debug!(file = ?file, "file finalized");
                            }
                            Err(err) => {
                                tracing::error!(error = %err, "error finalizing file");
                            }
                        }
                    }

                    // clean up everything and get the next_check time
                    {
                        let mut lock = pending.lock().unwrap();
                        lock.retain(|k, entry| !(keys.contains(k) && entry.file_writer.is_none()));
                        for (_, entry) in lock.iter() {
                            if entry.expiration < next_check {
                                next_check = entry.expiration;
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
            vfs,
            pending,
            max_idle,
            notify_finalizer,
            finalizer,
        }
    }

    pub async fn pending_file_by_id(&self, id: u64) -> Option<Result<File>> {
        self.pending_file(Either::Left(id)).await
    }

    pub async fn pending_file_by_dir_name(&self, parent: u64, name: String) -> Option<Result<File>> {
        self.pending_file(Either::Right((parent, name))).await
    }

    async fn pending_file(&self, option: Either<u64, (u64, String)>) -> Option<Result<File>> {
        let res: Either<Option<File>, Option<BoxFuture<Result<File>>>> = {
            let lock = self.pending.lock().expect("unable to acquire pending lock");
            match option {
                Either::Right((parent_id, name)) => {
                    Either::Right(lock.get(&(parent_id, name)).map(|e| {
                        let file = e.file.clone();
                        let mut ready = e.ready_rx.clone();
                        async move {
                            loop {
                                if let Some(file) = file.load_full() {
                                    return Ok(file.as_ref().clone());
                                }
                                ready.changed().await?;
                            }
                        }
                        .boxed()
                    }))
                }
                Either::Left(id) => Either::Left(
                    lock.values()
                        .find(|e| {
                            let file = e.file.load();
                            file.is_some() && file.as_ref().unwrap().id() == id
                        })
                        .map(|e| e.file.load_full().map(|f| f.as_ref().clone()))
                        .flatten(),
                ),
            }
        };

        match res {
            Either::Left(res) => res.map(|f| Ok(f)),
            Either::Right(Some(fut)) => Some(fut.await),
            Either::Right(None) => None,
        }
    }

    pub async fn add_upload(&self, parent: &Directory, name: String) -> Result<File> {
        let key = (parent.id(), name.clone());
        let (watch_tx, _) = watch::channel(0u64);
        let (ready_tx, ready_rx) = watch::channel(false);

        {
            let mut lock = self.pending.lock().expect("unable to acquire pending lock");
            if lock.contains_key(&key) {
                bail!("same upload already exists");
            }
            lock.insert(
                key.clone(),
                Entry {
                    file: Arc::new(ArcSwapOption::from(None)),
                    ready_rx,
                    file_writer: None,
                    expiration: SystemTime::now() + Duration::from_secs(3600),
                    watch_tx,
                },
            );
        }

        let res = self.vfs.write_file(&parent, name.to_string()).await;

        let mut lock = self.pending.lock().expect("unable to acquire pending lock");
        match res {
            Ok(file_writer) => {
                let file = file_writer.to_file();
                let entry = lock
                    .get_mut(&key)
                    .ok_or(anyhow!("entry not found in pending pool"))?;
                if entry.file.load().is_some() || entry.file_writer.is_some() {
                    // something went wrong here
                    bail!("expected entry file and entry file_writer to be none, but they are not");
                }
                entry.file.swap(Some(Arc::new(file.clone())));
                entry.file_writer = Some(file_writer);
                // indicate entry status is ready
                let _ = ready_tx.send(true);
                Ok(file)
            }
            Err(err) => {
                lock.remove(&key);
                Err(err)
            }
        }
    }

    pub fn get_upload(&self, file: &File, offset: u64) -> Result<BoxFuture<Result<Upload>>> {
        let key = (file.parent(), file.name().to_string());

        let mut watch_rx = {
            let mut lock = self.pending.lock().unwrap();
            let entry = lock.get_mut(&key).ok_or(anyhow!("upload not found"))?;

            // first, check if the upload is idle and the current write position matches our offset
            if entry.file_writer.is_some() && entry.watch_tx.borrow().deref() == &offset {
                // upload is idle & the current offset matches, try to take the writer
                if let Some(file_writer) = entry.file_writer.take() {
                    // double-check the offset
                    if file_writer.bytes_written() == offset {
                        // definitely a match
                        // return right away without waiting
                        let watch_tx = entry.watch_tx.clone();
                        let pool = self.pending.clone();
                        let max_idle = self.max_idle;
                        let notify_finalizer = self.notify_finalizer.clone();
                        return Ok(async move {
                            Ok(Upload {
                                file_writer: Some(file_writer),
                                pool,
                                watch_tx,
                                max_idle,
                                notify_finalizer,
                            })
                        }
                        .boxed());
                    }
                }
                bail!("invalid entry in pending upload pool");
            }
            // we need to queue and wait
            entry.watch_tx.subscribe()
        };

        let pending = self.pending.clone();
        let notify_finalizer = self.notify_finalizer.clone();
        let max_idle = self.max_idle;
        Ok(async move {
            loop {
                let bytes_written = *watch_rx.borrow_and_update();
                if bytes_written == offset {
                    let mut lock = pending.lock().unwrap();
                    if let Some(entry) = lock.get_mut(&key) {
                        if let Some(file_writer) = entry.file_writer.take() {
                            if file_writer.bytes_written() == offset {
                                // bingo
                                return Ok(Upload {
                                    file_writer: Some(file_writer),
                                    watch_tx: entry.watch_tx.clone(),
                                    pool: pending.clone(),
                                    notify_finalizer,
                                    max_idle,
                                });
                            }
                        }
                    }
                } else if bytes_written > offset {
                    bail!("bytes_written > offset: {} > {}", bytes_written, offset);
                }
                watch_rx.changed().await?;
            }
        }
        .boxed())

        /*if let Some(Some(entry)) = lock.get_mut(&key) {
            if let Some(file_writer) = entry.file_writer.take() {
                if file_writer.bytes_written() != offset {
                    let actual = file_writer.bytes_written();
                    // put it back
                    entry.file_writer = Some(file_writer);
                    bail!(
                        "file_writer offset is incorrect, expected {} but found {}",
                        offset,
                        actual
                    );
                }
                return Ok(Upload {
                    file_writer: Some(file_writer),
                    pool: self.pending.clone(),
                    max_idle: self.max_idle,
                    notify_finalizer: self.notify_finalizer.clone(),
                });
            }
        }
        bail!("invalid entry in pending upload pool");*/
    }
}

impl Drop for UploadManager {
    fn drop(&mut self) {
        self.finalizer.abort();
    }
}

pub(crate) struct Upload {
    file_writer: Option<FileWriter>,
    pool: Arc<Mutex<HashMap<(u64, String), Entry>>>,
    max_idle: Duration,
    notify_finalizer: Arc<Notify>,
    watch_tx: watch::Sender<u64>,
}

impl Upload {
    fn file_writer(&mut self) -> io::Result<&mut FileWriter> {
        self.file_writer
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "file writer gone"))
    }

    pub fn to_file(&self) -> Option<File> {
        self.file_writer.as_ref().map(|f| f.to_file())
    }
}

impl AsyncWrite for Upload {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.file_writer()?).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file_writer()?).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file_writer()?).poll_close(cx)
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        if let Some(file_writer) = self.file_writer.take() {
            let mut error = false;
            let bytes_written = file_writer.bytes_written();
            {
                let mut lock = self.pool.lock().unwrap();
                let key = (file_writer.parent_id(), file_writer.name().to_string());
                if let Some(entry) = lock.get_mut(&key) {
                    entry.file_writer = Some(file_writer);
                    entry.expiration = SystemTime::now() + self.max_idle;
                } else {
                    tracing::error!(
                        "unable to return file_writer to the pool, entry missing for {:?}",
                        key
                    );
                    error = true;
                }
            }
            self.notify_finalizer.notify_one();
            let _ = self.watch_tx.send(bytes_written);
            if !error {
                tracing::debug!("file_writer returned to pool");
            }
        }
    }
}
