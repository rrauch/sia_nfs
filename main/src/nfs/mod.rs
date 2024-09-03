mod upload;

use crate::io_scheduler::Scheduler;
use crate::nfs::upload::Upload;
use crate::vfs::inode::{Inode, InodeType};
use crate::vfs::Vfs;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use nfsserve::nfs::nfsstat3::{
    NFS3ERR_IO, NFS3ERR_ISDIR, NFS3ERR_NOENT, NFS3ERR_NOTDIR, NFS3ERR_NOTSUPP, NFS3ERR_SERVERFAULT,
};
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use std::io::SeekFrom;
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;

pub(crate) struct SiaNfsFs {
    vfs: Arc<Vfs>,
    uploader: Scheduler<Upload>,
}

impl SiaNfsFs {
    pub(super) fn new(vfs: Arc<Vfs>, upload_max_idle: Duration) -> Self {
        let uploader = Upload::new(vfs.clone(), upload_max_idle);
        Self { uploader, vfs }
    }
}

#[async_trait]
impl NFSFileSystem for SiaNfsFs {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    fn root_dir(&self) -> fileid3 {
        self.vfs.root().id()
    }

    #[instrument(skip(self))]
    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        Ok(self.inode_by_dir_name(dirid, filename).await?.id())
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        Ok(to_fattr3(&self.inode_by_id(id).await?))
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        tracing::debug!("setattr called");
        Ok(to_fattr3(&self.inode_by_id(id).await?))
    }

    #[instrument(skip(self))]
    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let file = match self.inode_by_id(id).await? {
            Inode::File(file) => file,
            Inode::Directory(_) => return Err(NFS3ERR_ISDIR),
        };

        // make sure we don't read beyond eof
        let count = {
            let count = count as u64;
            if offset + count >= file.size() {
                (file.size() - offset) as usize
            } else {
                count as usize
            }
        };

        if count == 0 {
            return Ok((vec![], true));
        }

        let mut file_reader = self.vfs.read_file(&file).await.map_err(|e| {
            tracing::error!(error = %e, "failed to call read_file for file {}", id);
            NFS3ERR_SERVERFAULT
        })?;
        let _ = file_reader
            .seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to seek to offset {} for file {}", offset, id);
                NFS3ERR_SERVERFAULT
            })?;

        let mut buf = Vec::with_capacity(count);
        buf.resize(buf.capacity(), 0x00);
        file_reader.read_exact(&mut buf).await.map_err(|e| {
            tracing::error!(error = %e, "read error");
            NFS3ERR_IO
        })?;

        let eof = file_reader.eof();

        Ok((buf, file_reader.eof()))
    }

    #[instrument(skip(self, data), fields(count = data.len()))]
    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        let file = match self.inode_by_id(id).await? {
            Inode::File(file) => file,
            Inode::Directory(_) => return Err(NFS3ERR_ISDIR),
        };

        let mut upload = self
            .uploader
            .access(&file.id(), offset)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to acquire upload handle for {}", file.id());
                NFS3ERR_NOENT
            })?;
        let upload = upload.as_mut();

        upload.write_all(data).await.map_err(|e| {
            tracing::error!(error = %e, "write error");
            NFS3ERR_IO
        })?;

        tracing::debug!(file = ?file, offset = offset, data = data.len(), "write complete");

        Ok(to_fattr3(&Inode::File(upload.to_file())))
    }

    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let id = self.create_exclusive(dirid, filename).await?;
        let inode = self.inode_by_id(id).await?;
        if inode.inode_type() != InodeType::File {
            return Err(NFS3ERR_ISDIR);
        }
        Ok((id, to_fattr3(&inode)))
    }

    #[instrument(skip(self))]
    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let name = to_str(filename)?;
        let parent = match self.inode_by_id(dirid).await? {
            Inode::File(_) => return Err(NFS3ERR_NOTDIR),
            Inode::Directory(dir) => dir,
        };

        let file_id = self
            .uploader
            .prepare(&(parent.id(), name.to_string()))
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to prepare upload");
                NFS3ERR_IO
            })?;

        Ok(file_id)
    }

    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let name = to_str(dirname)?;
        let parent = match self.inode_by_id(dirid).await? {
            Inode::File(_) => return Err(NFS3ERR_NOTDIR),
            Inode::Directory(dir) => dir,
        };

        let dir = self
            .vfs
            .mkdir(&parent, name.to_string())
            .await
            .map_err(|_| NFS3ERR_SERVERFAULT)?;

        Ok((dir.id(), to_fattr3(&Inode::Directory(dir))))
    }

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        let inode = self.inode_by_dir_name(dirid, filename).await?;

        self.vfs.rm(&inode).await.map_err(|e| {
            tracing::error!(err = %e, "rm failed");
            NFS3ERR_SERVERFAULT
        })?;

        Ok(())
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        let source = self.inode_by_dir_name(from_dirid, from_filename).await?;
        let dest_dir = match self.inode_by_id(to_dirid).await? {
            Inode::File(_) => return Err(NFS3ERR_NOTDIR),
            Inode::Directory(dir) => dir,
        };
        let to_filename = to_str(to_filename)?;
        let dest_name = if to_filename == source.name() {
            // no name change
            None
        } else {
            Some(to_filename.to_string())
        };

        self.vfs
            .mv(&source, &dest_dir, dest_name)
            .await
            .map_err(|e| {
                tracing::error!(err = %e, "mv failed");
                NFS3ERR_SERVERFAULT
            })?;

        Ok(())
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let dir = match self.inode_by_id(dirid).await? {
            Inode::Directory(dir) => dir,
            _ => return Err(NFS3ERR_NOTDIR),
        };

        let inodes = self.vfs.read_dir(&dir).await.map_err(|err| {
            tracing::error!(error = %err, "read_dir failed");
            NFS3ERR_SERVERFAULT
        })?;

        let mut ret = ReadDirResult {
            entries: Vec::new(),
            end: false,
        };

        let mut start_index = 0;
        if start_after > 0 {
            if let Some(pos) = inodes.iter().position(|inode| inode.id() == start_after) {
                start_index = pos + 1;
            } else {
                return Err(nfsstat3::NFS3ERR_BAD_COOKIE);
            }
        }
        let remaining_length = inodes.len() - start_index;

        for inode in inodes[start_index..].iter() {
            ret.entries.push(DirEntry {
                fileid: inode.id(),
                name: inode.name().as_bytes().into(),
                attr: to_fattr3(inode),
            });
            if ret.entries.len() >= max_entries {
                break;
            }
        }
        if ret.entries.len() == remaining_length {
            ret.end = true;
        }

        Ok(ret)
    }

    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(NFS3ERR_NOTSUPP)
    }

    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        Err(NFS3ERR_NOTSUPP)
    }
}

impl SiaNfsFs {
    async fn inode_by_id(&self, id: fileid3) -> Result<Inode, nfsstat3> {
        match self
            .vfs
            .inode_by_id(id)
            .await
            .map_err(|_| NFS3ERR_SERVERFAULT)?
        {
            Some(inode) => Ok(inode),
            None => Err(NFS3ERR_NOENT),
        }
    }

    async fn inode_by_dir_name(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<Inode, nfsstat3> {
        let name = to_str(filename)?;
        match self
            .vfs
            .inode_by_name_parent(name, dirid)
            .await
            .map_err(|e| {
                tracing::error!(err = %e, "lookup failed");
                NFS3ERR_SERVERFAULT
            })? {
            Some(inode) => Ok(inode),
            None => Err(NFS3ERR_NOENT),
        }
    }
}

fn to_fattr3(inode: &Inode) -> fattr3 {
    let size = match inode {
        Inode::Directory(_) => 0,
        Inode::File(file) => file.size(),
    };
    let last_modified = to_nfsstime(inode.last_modified());

    fattr3 {
        ftype: match inode {
            Inode::Directory(_) => ftype3::NF3DIR,
            Inode::File(_) => ftype3::NF3REG,
        },
        mode: match inode {
            Inode::Directory(_) => 0o700,
            Inode::File(_) => 0o600,
        },
        nlink: 1,
        uid: 1000,
        gid: 1000,
        size,
        used: size,
        rdev: specdata3::default(),
        fsid: 0,
        fileid: inode.id(),
        atime: last_modified,
        mtime: last_modified,
        ctime: last_modified,
    }
}

fn to_nfsstime(date_time: &DateTime<Utc>) -> nfstime3 {
    nfstime3 {
        seconds: date_time.timestamp() as u32,
        nseconds: date_time.timestamp_subsec_nanos(),
    }
}

fn to_str(name: &filename3) -> Result<&str, nfsstat3> {
    Ok(std::str::from_utf8(name).map_err(|_| NFS3ERR_SERVERFAULT)?)
}
