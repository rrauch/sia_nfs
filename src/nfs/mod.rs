mod download;

use crate::nfs::download::DownloadManager;
use crate::vfs::{Inode, Vfs};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::AsyncReadExt;
use nfsserve::nfs::nfsstat3::{
    NFS3ERR_IO, NFS3ERR_ISDIR, NFS3ERR_NOENT, NFS3ERR_NOTDIR, NFS3ERR_NOTSUPP, NFS3ERR_SERVERFAULT,
};
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;

pub(crate) struct SiaNfsFs {
    vfs: Arc<Vfs>,
    download_manager: DownloadManager,
}

impl SiaNfsFs {
    pub(super) fn new(
        vfs: Vfs,
        max_downloads_per_file: NonZeroUsize,
        max_download_idle: Duration,
    ) -> Self {
        let vfs = Arc::new(vfs);
        let download_manager =
            DownloadManager::new(vfs.clone(), max_downloads_per_file, max_download_idle);
        Self {
            download_manager,
            vfs,
        }
    }
}

#[async_trait]
impl NFSFileSystem for SiaNfsFs {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    fn root_dir(&self) -> fileid3 {
        self.vfs.root().id()
    }

    #[instrument(skip(self))]
    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let name = std::str::from_utf8(filename).map_err(|_| NFS3ERR_SERVERFAULT)?;
        match self
            .vfs
            .inode_by_name_parent(name, dirid)
            .await
            .map_err(|e| {
                tracing::error!(err = %e, "lookup failed");
                NFS3ERR_SERVERFAULT
            })? {
            Some(inode) => Ok(inode.id()),
            None => Err(NFS3ERR_NOENT),
        }
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        match self
            .vfs
            .inode_by_id(id)
            .await
            .map_err(|_| NFS3ERR_SERVERFAULT)?
        {
            Some(inode) => Ok(to_fattr3(&inode)),
            None => Err(NFS3ERR_NOENT),
        }
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(NFS3ERR_NOTSUPP)
    }

    #[instrument(skip(self))]
    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let file = match self
            .vfs
            .inode_by_id(id)
            .await
            .map_err(|_| NFS3ERR_SERVERFAULT)?
        {
            Some(Inode::File(file)) => file,
            Some(Inode::Directory(_)) => return Err(NFS3ERR_ISDIR),
            None => return Err(NFS3ERR_NOENT),
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

        let mut dl = self
            .download_manager
            .download(&file, offset)
            .await
            .map_err(|_| NFS3ERR_SERVERFAULT)?;

        let mut buf = Vec::with_capacity(count);
        buf.resize(buf.capacity(), 0x00);
        dl.read_exact(&mut buf).await.map_err(|e| {
            tracing::error!(error = %e, "read error");
            NFS3ERR_IO
        })?;

        Ok((buf, dl.eof()))
    }

    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        Err(NFS3ERR_NOTSUPP)
    }

    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(NFS3ERR_NOTSUPP)
    }

    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        Err(NFS3ERR_NOTSUPP)
    }

    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(NFS3ERR_NOTSUPP)
    }

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        Err(NFS3ERR_NOTSUPP)
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        Err(NFS3ERR_NOTSUPP)
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let dir = match self
            .vfs
            .inode_by_id(dirid)
            .await
            .map_err(|_| NFS3ERR_SERVERFAULT)?
        {
            Some(Inode::Directory(dir)) => dir,
            Some(_) => return Err(NFS3ERR_NOTDIR),
            None => return Err(NFS3ERR_NOENT),
        };

        let inodes = self
            .vfs
            .read_dir(&dir)
            .await
            .map_err(|_| NFS3ERR_SERVERFAULT)?;

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
