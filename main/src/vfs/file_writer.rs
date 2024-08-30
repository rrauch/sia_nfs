use crate::vfs::inode::{File, Inode};
use crate::vfs::locking::LockHolder;
use crate::vfs::{PendingWrites, Vfs};
use anyhow::anyhow;
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::AsyncWrite;
use futures_util::AsyncWriteExt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncWrite as TokioAsyncWrite, DuplexStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub(crate) struct FileWriter {
    id: u64,
    name: String,
    parent: u64,
    bucket: String,
    path: String,
    stream: Option<DuplexStream>,
    _locks: Vec<LockHolder>,
    upload_task: Option<JoinHandle<std::result::Result<(), renterd_client::Error>>>,
    vfs: Arc<Vfs>,
    bytes_written: u64,
    last_modified: DateTime<Utc>,
    error_count: usize,
    file_tx: watch::Sender<File>,
    pending_writes: Arc<PendingWrites>,
}

impl AsyncWrite for FileWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(self.stream()?).poll_write(cx, buf);
        if let Poll::Ready(res) = &result {
            match res {
                Ok(bytes_written) => {
                    self.bytes_written += *bytes_written as u64;
                }
                Err(_) => {
                    self.error_count += 1;
                }
            }
            self.update_file();
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let result = Pin::new(self.stream()?).poll_flush(cx);
        if let Poll::Ready(res) = &result {
            if let Err(_) = res {
                self.error_count += 1;
            }
            self.update_file();
        }
        result
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let result = Pin::new(self.stream()?).poll_shutdown(cx);
        if let Poll::Ready(res) = &result {
            match res {
                Ok(()) => {
                    // was properly closed, discard stream
                    let _ = self.stream.take();
                    tracing::debug!("FileWriter for /{}{} closed", self.bucket, self.path);
                }
                Err(_) => {
                    self.error_count += 1;
                }
            }
            self.update_file();
        }
        result
    }
}

impl FileWriter {
    pub(super) fn new(
        reserved_id: u64,
        name: String,
        bucket: String,
        path: String,
        parent_id: u64,
        vfs: Arc<Vfs>,
        locks: Vec<LockHolder>,
        upload_task: JoinHandle<std::result::Result<(), renterd_client::Error>>,
        stream: DuplexStream,
        file_tx: watch::Sender<File>,
        pending_writes: Arc<PendingWrites>,
    ) -> Self {
        Self {
            id: reserved_id,
            vfs,
            name,
            bucket,
            path,
            parent: parent_id,
            _locks: locks,
            upload_task: Some(upload_task),
            stream: Some(stream),
            bytes_written: 0,
            last_modified: Utc::now(),
            error_count: 0,
            file_tx,
            pending_writes,
        }
    }

    fn stream(&mut self) -> std::io::Result<&mut DuplexStream> {
        self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "stream already closed")
        })
    }

    pub fn file_id(&self) -> u64 {
        self.id
    }

    pub fn parent_id(&self) -> u64 {
        self.parent
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn bucket(&self) -> &str {
        self.bucket.as_str()
    }

    pub fn path(&self) -> &str {
        self.path.as_str()
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn error_count(&self) -> usize {
        self.error_count
    }

    pub fn is_closed(&self) -> bool {
        self.stream.is_none()
    }

    pub fn to_file(&self) -> File {
        File::new(
            self.id,
            self.name.clone(),
            self.bytes_written,
            self.last_modified,
            self.parent,
        )
    }

    fn update_file(&self) {
        let mut file = self.file_tx.borrow().clone();
        file.set_size(self.bytes_written);
        file.set_last_modified(Utc::now());
        let _ = self.file_tx.send(file);
    }

    pub async fn finalize(mut self) -> Result<File> {
        if !self.is_closed() {
            // stream has not been closed yet
            self.close().await?;
        }

        // wait for the upload task to finish
        self.upload_task
            .take()
            .expect("JoinHandle went missing")
            .await??;

        // add the new file to the db using the id we previously "reserved"
        self.vfs
            .inode_manager
            .new_file(self.id, self.name.clone(), self.parent_id())
            .await?;

        // update caches
        self.vfs
            .cache_manager
            .new_file(self.id, self.parent_id())
            .await;

        let file = match self
            .vfs
            .inode_by_name_parent(&self.name, self.parent)
            .await?
        {
            Some(Inode::File(file)) => Ok(file),
            _ => Err(anyhow!("uploaded inode invalid")),
        }?;

        let (bucket, path) = self
            .vfs
            .inode_to_bucket_path(Inode::File(file.clone()))
            .await?
            .ok_or(anyhow!("uploaded inode invalid"))?;

        self.vfs
            .cache_manager
            .invalidate_bucket_path(bucket, path)
            .await;

        Ok(file)
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        if !self.is_closed() {
            tracing::warn!(
                "FileWriter for /{}{} dropped before closing, data corruption possible!",
                self.bucket,
                self.path
            );
        }
        if let Some(upload_task) = self.upload_task.take() {
            upload_task.abort();
            tracing::warn!(
                "upload_task for /{}{} had to be aborted, data corruption possible!",
                self.bucket,
                self.path
            )
        }
        self.pending_writes.remove(self.id, self.parent);
    }
}
