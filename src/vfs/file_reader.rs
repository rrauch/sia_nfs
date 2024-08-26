use crate::vfs::inode::File;
use crate::vfs::locking::ReadLock;
use futures::{AsyncRead, AsyncSeek};
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::OwnedSemaphorePermit;
use tracing::instrument;

pub struct FileReader {
    file: File,
    _read_lock: ReadLock,
    _download_permit: OwnedSemaphorePermit,
    offset: u64,
    size: u64,
    stream: Box<dyn ReadStream + Send + Unpin>,
    error_count: usize,
}

impl FileReader {
    pub(super) fn new(
        file: File,
        size: u64,
        offset: u64,
        stream: impl AsyncRead + AsyncSeek + Send + Unpin + 'static,
        read_lock: ReadLock,
        download_permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            file,
            _read_lock: read_lock,
            _download_permit: download_permit,
            offset,
            size,
            stream: Box::new(stream),
            error_count: 0,
        }
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn eof(&self) -> bool {
        self.offset >= self.size
    }

    pub fn error_count(&self) -> usize {
        self.error_count
    }
}

trait ReadStream: AsyncRead + AsyncSeek {}
impl<T: AsyncRead + AsyncSeek> ReadStream for T {}

impl AsyncRead for FileReader {
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

impl AsyncSeek for FileReader {
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

impl Drop for FileReader {
    #[instrument(skip(self), name = "file_reader_drop")]
    fn drop(&mut self) {
        tracing::debug!(
            id = self.file.id(),
            name = self.file.name(),
            offset = self.offset(),
            "file_reader closed"
        );
    }
}
