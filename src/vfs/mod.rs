mod cache;
pub(crate) mod file_reader;
pub(crate) mod file_writer;
pub(crate) mod inode;
mod locking;

use crate::vfs::cache::CacheManager;
use crate::vfs::file_reader::FileReader;
use crate::vfs::file_writer::FileWriter;
use crate::vfs::inode::{Directory, File, Inode, InodeManager, InodeType};
use crate::vfs::locking::{LockHolder, LockManager, LockRequest};
use crate::SqlitePool;
use anyhow::{anyhow, bail, Result};
use async_recursion::async_recursion;
use chrono::Utc;
use futures::pin_mut;
use futures_util::io::Cursor;
use futures_util::{StreamExt, TryStreamExt};
use itertools::{Either, Itertools};
use parking_lot::RwLock;
use renterd_client::bus::object::{Metadata, RenameMode};
use renterd_client::Client as RenterdClient;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Semaphore};
use tokio::time::timeout;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{info, instrument};

const ROOT_ID: u64 = 1;

pub(crate) struct Vfs {
    renterd: RenterdClient,
    inode_manager: InodeManager,
    root: Directory,
    download_limiter: Arc<Semaphore>,
    lock_manager: LockManager,
    cache_manager: CacheManager,
    pending_writes: Arc<PendingWrites>,
}

impl Vfs {
    pub(super) async fn new<T, I>(
        renterd: RenterdClient,
        db: SqlitePool,
        buckets: T,
        max_concurrent_downloads: NonZeroUsize,
    ) -> Result<Self>
    where
        T: IntoIterator<Item = I>,
        I: ToString,
    {
        // get all available buckets from renterd
        let available_buckets = renterd
            .bus()
            .bucket()
            .get_all()
            .await?
            .into_iter()
            .map(|b| (b.name, b.created_at.to_utc()))
            // sort by date (oldest first) and if the same by name, alphabetically
            .sorted_by(|a, b| match a.1.cmp(&b.1) {
                std::cmp::Ordering::Equal => a.0.cmp(&b.0),
                other => other,
            })
            .collect::<BTreeMap<_, _>>();

        // map selected buckets
        let buckets = buckets
            .into_iter()
            .try_fold(BTreeMap::new(), |mut buckets, b| {
                let name = b.to_string();
                if let Some(last_modified) = available_buckets.get(&name) {
                    buckets.insert(
                        name.clone(),
                        Directory::new(0, name, last_modified.clone(), ROOT_ID),
                    );
                    Ok(buckets)
                } else {
                    Err(anyhow!("unknown bucket: '{}', check your config", name))
                }
            })?;

        let inode_manager = InodeManager::new(db, ROOT_ID, buckets).await?;
        let download_limiter = Arc::new(Semaphore::new(max_concurrent_downloads.get()));

        Ok(Self {
            renterd,
            inode_manager,
            root: Directory::new(ROOT_ID, "root".to_string(), Utc::now(), ROOT_ID),
            download_limiter,
            lock_manager: LockManager::new(Duration::from_secs(30)),
            cache_manager: CacheManager::new(
                10_000,
                100_000,
                Duration::from_secs(600),
                Duration::from_secs(180),
                Duration::from_secs(10),
            ),
            pending_writes: Arc::new(PendingWrites::new()),
        })
    }

    pub async fn inode_by_name_parent<S: AsRef<str>>(
        &self,
        name: S,
        parent_id: u64,
    ) -> Result<Option<Inode>> {
        let parent = match self.inode_by_id(parent_id).await? {
            Some(Inode::Directory(dir)) => dir,
            _ => bail!("invalid parent"),
        };
        let name = name.as_ref();

        if parent_id == ROOT_ID {
            return Ok(self
                .inode_manager
                .bucket_by_name(name)
                .map(|dir| Inode::Directory(dir.clone())));
        };

        // check pending writes first
        if let Some(file) = self.pending_writes.by_name_parent(name, parent_id) {
            return Ok(Some(Inode::File(file)));
        }

        let (bucket, path) = self
            .inode_to_bucket_path(Inode::Directory(parent))
            .await?
            .ok_or(anyhow!("unable to find bucket/path for parent"))?;

        let id = match self
            .inode_id_by_bucket_path(bucket, format!("{}{}", path, name), parent_id, path)
            .await?
        {
            Some(id) => id,
            None => return Ok(None),
        };

        self.inode_by_id(id).await
    }

    async fn inode_id_by_bucket_path(
        &self,
        bucket: String,
        path: String,
        parent_id: u64,
        parent_path: String,
    ) -> Result<Option<u64>> {
        let path = path.trim_end_matches('/');
        let bucket_path = (bucket, path.to_string());
        self.cache_manager
            .inode_id_by_bucket_path(
                &bucket_path,
                &parent_path,
                self._inode_by_bucket_path(&bucket_path.0, &bucket_path.1, parent_id),
            )
            .await
    }

    async fn _inode_by_bucket_path(
        &self,
        bucket: &str,
        path: &str,
        parent_id: u64,
    ) -> Result<Option<Inode>> {
        let path = path.trim_end_matches('/');
        let (_, name) = path.rsplit_once('/').ok_or(anyhow!("invalid path"))?;
        if name.is_empty() {
            bail!("invalid path, name is empty");
        }

        let mut stream = self.renterd.bus().object().list(
            NonZeroUsize::new(1).unwrap(),
            Some(path.to_string()),
            Some(bucket.to_string()),
        )?;

        let metadata = match stream.try_next().await? {
            Some(mut matches) if matches.len() == 1 => matches.remove(0),
            Some(matches) if matches.is_empty() => {
                return Ok(None);
            }
            None => {
                return Ok(None);
            }
            _ => {
                bail!("unexpected number of matches returned");
            }
        };

        if metadata.name != path && metadata.name != format!("{}/", path) {
            // this is a different entry
            return Ok(None);
        }

        let inode_type = if metadata.name.ends_with('/') {
            InodeType::Directory
        } else {
            InodeType::File
        };

        Ok(Some(
            self.inode_manager
                .sync_by_name_parent(
                    name,
                    parent_id,
                    inode_type,
                    metadata.size,
                    metadata.mod_time.to_utc(),
                )
                .await?,
        ))
    }

    pub async fn inode_by_id(&self, id: u64) -> Result<Option<Inode>> {
        // first, check for pending writes
        if let Some(file) = self.pending_writes.by_id(id) {
            return Ok(Some(Inode::File(file)));
        }

        self.cache_manager
            .inode_by_id(id, self._inode_by_id(id))
            .await
    }

    async fn _inode_by_id(&self, id: u64) -> Result<Option<Inode>> {
        if id < ROOT_ID {
            return Ok(None);
        }
        if id == ROOT_ID {
            return Ok(Some(Inode::Directory(self.root.clone())));
        } else if let Some(dir) = self.inode_manager.bucket_by_id(id) {
            return Ok(Some(Inode::Directory(dir.clone())));
        }

        let inode = match { self.inode_manager.inode_by_id(id).await? } {
            Some(inode) => inode,
            None => return Ok(None),
        };

        let parent_id = inode.parent();

        let (bucket, path) = match self.inode_to_bucket_path(inode).await? {
            Some((bucket, path)) => (bucket, path),
            None => return Ok(None),
        };

        self._inode_by_bucket_path(&bucket, &path, parent_id).await
    }

    async fn inode_to_bucket_path(&self, inode: Inode) -> Result<Option<(String, String)>> {
        let id = inode.id();
        let is_dir = inode.inode_type() == InodeType::Directory;
        Ok(self
            .cache_manager
            .inode_to_bucket_path(id, self._inode_to_bucket_path(inode))
            .await?
            .map(|(bucket, mut path)| {
                if is_dir && !path.ends_with('/') {
                    path = format!("{}/", path);
                }
                (bucket, path)
            }))
    }

    #[async_recursion]
    async fn _inode_to_bucket_path(&self, inode: Inode) -> Result<Option<(String, String)>> {
        let mut bucket = None;
        let mut components = vec![];
        let mut inode = Some(inode);
        let mut last_is_dir = false;
        let mut counter = 0usize;

        while let Some(current_inode) = inode {
            let parent_id;
            match current_inode {
                Inode::Directory(dir) => {
                    if dir.id() == ROOT_ID {
                        // reached root
                        break;
                    }
                    if dir.parent() == ROOT_ID {
                        // this is the bucket
                        bucket = Some(dir.name().to_string());
                        break;
                    } else {
                        if counter == 0 {
                            last_is_dir = true;
                        }
                        components.push(dir.name().to_string());
                        parent_id = dir.parent();
                    }
                }
                Inode::File(file) => {
                    assert_ne!(
                        file.parent(),
                        ROOT_ID,
                        "invalid fs, root can only contain buckets"
                    );
                    components.push(file.name().to_string());
                    parent_id = file.parent();
                }
            }
            counter += 1;
            inode = self.inode_by_id(parent_id).await?;
        }

        let bucket = match bucket {
            Some(bucket) => bucket,
            None => {
                return Ok(None);
            }
        };

        components.reverse();
        let tail = if last_is_dir && !components.is_empty() {
            "/"
        } else {
            ""
        };
        Ok(Some((
            bucket,
            format!("/{}{}", components.iter().join("/"), tail),
        )))
    }

    pub fn root(&self) -> &Directory {
        &self.root
    }

    pub async fn read_dir(&self, dir: &Directory) -> Result<Vec<Inode>> {
        if dir.id() == ROOT_ID {
            return Ok(self
                .inode_manager
                .buckets()
                .map(|d| Inode::Directory(d.clone()))
                .collect::<Vec<_>>());
        }
        let dir_id = dir.id();
        let (bucket, path) = self
            .inode_to_bucket_path(Inode::Directory(dir.clone()))
            .await?
            .ok_or(anyhow!("invalid dir"))?;

        let _locks = self
            .lock_manager
            .lock([LockRequest::read(&bucket, &path)])
            .await?;

        let mut inodes = self
            .cache_manager
            .read_dir(
                dir_id,
                bucket.clone(),
                path.clone(),
                self._read_dir(dir_id, bucket, path),
            )
            .await?;

        // overlay any pending writes
        inodes.extend(
            self.pending_writes
                .by_parent(dir_id)
                .into_iter()
                .map(|f| Inode::File(f)),
        );

        Ok(inodes)
    }

    async fn _read_dir(
        &self,
        dir_id: u64,
        bucket: String,
        path: String,
    ) -> Result<(Vec<Inode>, Vec<u64>)> {
        let stream = match self
            .renterd
            .bus()
            .object()
            .get_stream(
                path.as_str(),
                NonZeroUsize::new(1000).unwrap(),
                None,
                Some(bucket.clone()),
            )
            .await?
            .ok_or(anyhow!("path not found"))?
        {
            Either::Left(_file) => bail!("expected a directory but got a file"),
            Either::Right(stream) => stream,
        };

        let stream = stream
            .try_filter_map(|m| async {
                Ok(Some(
                    m.into_iter()
                        .filter_map(|m| Inode::try_from((m, path.as_str(), dir_id)).ok())
                        .collect_vec(),
                ))
            })
            .map_err(|e| e.into());

        pin_mut!(stream);

        self.inode_manager.sync_dir(dir_id, stream).await
    }

    pub async fn read_file(
        &self,
        file: &File,
        offset: impl Into<Option<u64>>,
    ) -> Result<FileReader> {
        let (bucket, path) = self
            .inode_to_bucket_path(Inode::File(file.clone()))
            .await?
            .ok_or(anyhow!("invalid file"))?;

        // acquire the actual read lock
        let read_lock = match self
            .lock_manager
            .lock([LockRequest::read(&bucket, &path)])
            .await?
            .swap_remove(0)
        {
            LockHolder::Read(read_lock) => read_lock,
            _ => unreachable!("not a read lock"),
        };

        tracing::trace!("waiting for download permit");
        let download_permit = timeout(
            Duration::from_secs(60),
            self.download_limiter.clone().acquire_owned(),
        )
        .await??;
        tracing::trace!("download permit acquired");

        let object = self
            .renterd
            .worker()
            .object()
            .download(path, Some(bucket))
            .await?
            .ok_or(anyhow!("renterd couldn't find the file"))?;

        let offset = offset.into().unwrap_or(0);
        let size = object.length.ok_or(anyhow!("file size is unknown"))?;
        let stream = object.open_seekable_stream(offset).await?;

        Ok(FileReader::new(
            file.clone(),
            size,
            offset,
            stream,
            read_lock,
            download_permit,
        ))
    }

    pub async fn write_file(
        self: &Arc<Self>,
        parent: &Directory,
        name: String,
    ) -> Result<FileWriter> {
        if parent.id() == self.root.id() {
            bail!("unable to write in root directory");
        }

        let (bucket, parent_path) = self
            .inode_to_bucket_path(Inode::Directory(parent.clone()))
            .await?
            .ok_or(anyhow!("directory not found"))?;

        if name.ends_with("/") {
            bail!("invalid name");
        }
        let path = format!("{}{}", parent_path, name);

        // lock the paths first
        let locks = self
            .lock_manager
            .lock([
                LockRequest::read(&bucket, &parent_path),
                LockRequest::write(&bucket, &path),
            ])
            .await?;

        if let Some(_) = self
            .inode_by_name_parent(name.as_str(), parent.id())
            .await?
        {
            // an inode with the same name already exists in the parent directory
            bail!("inode already exists");
        }

        let reserved_id = self.inode_manager.reserve_id(&name, parent.id()).await?;

        let file_tx = {
            let (file_tx, file_rx) = watch::channel(File::new(
                reserved_id,
                name.clone(),
                0,
                Utc::now(),
                parent.id(),
            ));

            self.pending_writes
                .insert(reserved_id, parent.id(), file_rx);
            file_tx
        };

        let (tx, rx) = tokio::io::duplex(8192);
        let rx = rx.compat();

        let upload_task = {
            let vfs = self.clone();
            let bucket = bucket.clone();
            let path = path.clone();
            tokio::spawn(async move {
                vfs.renterd
                    .worker()
                    .object()
                    .upload(path, None, Some(bucket), rx)
                    .await
            })
        };

        Ok(FileWriter::new(
            reserved_id,
            name,
            bucket,
            path,
            parent.id(),
            self.clone(),
            locks,
            upload_task,
            tx,
            file_tx,
            self.pending_writes.clone(),
        ))
    }

    #[instrument(skip(self))]
    pub async fn mkdir(&self, parent: &Directory, name: String) -> Result<Directory> {
        if parent.id() == self.root.id() {
            bail!("unable to write in root directory");
        }

        let (bucket, parent_path) = self
            .inode_to_bucket_path(Inode::Directory(parent.clone()))
            .await?
            .ok_or(anyhow!("directory not found"))?;

        if name.ends_with("/") {
            bail!("invalid name");
        }
        let path = format!("{}{}/", parent_path, name);

        // lock the paths first
        let _locks = self
            .lock_manager
            .lock([
                LockRequest::read(&bucket, &parent_path),
                LockRequest::write(&bucket, &path),
            ])
            .await?;

        if let Some(_) = self
            .inode_by_name_parent(name.as_str(), parent.id())
            .await?
        {
            // an inode with the same name already exists in the parent directory
            bail!("inode already exists");
        }

        // a directory is created by uploading an empty file with a name ending in "/"
        self.renterd
            .worker()
            .object()
            .upload(
                path.as_str(),
                None,
                Some(bucket.clone()),
                Cursor::new(vec![]),
            )
            .await?;

        self.cache_manager
            .new_dir(
                bucket.clone(),
                path.trim_end_matches('/').to_string(),
                parent.id(),
            )
            .await;

        // looking good
        match self.inode_by_name_parent(name, parent.id()).await? {
            Some(Inode::Directory(dir)) => {
                info!("created directory /{}{} ({})", bucket, path, dir.id());
                Ok(dir)
            }
            _ => Err(anyhow!("directory creation failed")),
        }
    }

    #[instrument(skip(self))]
    pub async fn rm(&self, inode: &Inode) -> Result<()> {
        if inode.parent() == self.root.id() {
            bail!("unable to write in root directory");
        }

        let (bucket, path) = self
            .inode_to_bucket_path(inode.clone())
            .await?
            .ok_or(anyhow!("inode not found"))?;

        let (_, parent_path) = self
            .inode_to_bucket_path(
                self.inode_by_id(inode.parent())
                    .await?
                    .ok_or(anyhow!("parent not found"))?,
            )
            .await?
            .ok_or(anyhow!("parent not found"))?;

        let _locks = self
            .lock_manager
            .lock([
                LockRequest::write(&bucket, &path),
                LockRequest::read(&bucket, &parent_path),
            ])
            .await?;

        // do a live-lookup first to make sure the local state is accurate
        match self
            .renterd
            .bus()
            .object()
            .get_stream(
                path.as_str(),
                NonZeroUsize::new(10).unwrap(),
                None,
                Some(bucket.clone()),
            )
            .await?
            .ok_or(anyhow!("path not found"))?
        {
            Either::Left(_) => {
                if inode.inode_type() != InodeType::File {
                    //todo
                    bail!("unexpected type, expected directory but found file");
                }
            }
            Either::Right(stream) => {
                if inode.inode_type() != InodeType::Directory {
                    //todo
                    bail!("unexpected type, expected file but found directory");
                }

                if stream.count().await != 0 {
                    // the directory is not empty
                    bail!("directory not empty");
                }
            }
        };

        // things are looking good, proceed with deletion

        self.renterd
            .worker()
            .object()
            .delete(path.as_str(), Some(bucket.clone()), false)
            .await?;

        // track all affected ids by this deletion
        let mut affected_ids = HashSet::new();
        affected_ids.insert(inode.parent());
        affected_ids.insert(inode.id());

        // update the database
        affected_ids.extend(self.inode_manager.delete(inode.id()).await?);

        self.cache_manager.invalidate_caches(affected_ids);

        info!("deleted /{}{} ({})", bucket, path, inode.id());

        Ok(())
    }

    pub async fn mv(
        &self,
        source: &Inode,
        target_dir: &Directory,
        target_name: Option<String>,
    ) -> Result<()> {
        if source.parent() == self.root.id() || target_dir.id() == self.root.id() {
            bail!("unable to write in root directory");
        }

        if source.parent() == target_dir.id() {
            if target_name.is_none() || target_name.as_ref().unwrap() == source.name() {
                // nothing to do here
                return Ok(());
            }
        }

        if source.id() == target_dir.id() {
            bail!("cannot move into self");
        }

        let (bucket, source_path) = self
            .inode_to_bucket_path(source.clone())
            .await?
            .ok_or(anyhow!("inode not found"))?;

        let (_, source_parent_path) = self
            .inode_to_bucket_path(
                self.inode_by_id(source.parent())
                    .await?
                    .ok_or(anyhow!("parent not found"))?,
            )
            .await?
            .ok_or(anyhow!("parent not found"))?;

        let target_name = target_name.unwrap_or(source.name().to_string());

        let (target_bucket, target_parent_path) = self
            .inode_to_bucket_path(Inode::Directory(target_dir.clone()))
            .await?
            .ok_or(anyhow!("inode not found"))?;

        if target_bucket != bucket {
            bail!("cannot move between buckets");
        }

        let rename_mode = match source.inode_type() {
            InodeType::File => RenameMode::Single,
            InodeType::Directory => RenameMode::Multi,
        };

        let tail = if source.inode_type() == InodeType::Directory {
            "/"
        } else {
            ""
        };
        let target_path = format!("{}{}{}", target_parent_path, target_name, tail);

        let _locks = self
            .lock_manager
            .lock([
                LockRequest::read(bucket.as_str(), source_parent_path.as_str()),
                LockRequest::write(bucket.as_str(), source_path.as_str()),
                LockRequest::read(bucket.as_str(), target_parent_path.as_str()),
                LockRequest::write(bucket.as_str(), target_path.as_str()),
            ])
            .await?;

        // live-check first
        // make sure that the source exists
        if !{
            match self
                .renterd
                .bus()
                .object()
                .list(
                    NonZeroUsize::new(1).unwrap(),
                    Some(source_path.clone()),
                    Some(bucket.clone()),
                )?
                .try_next()
                .await
            {
                Ok(Some(_)) => true,
                _ => false,
            }
        } {
            bail!("source does not exist");
        }

        // and that the target does not
        if {
            match self
                .renterd
                .bus()
                .object()
                .list(
                    NonZeroUsize::new(1).unwrap(),
                    Some(target_path.clone()),
                    Some(bucket.clone()),
                )?
                .try_next()
                .await
            {
                Ok(Some(metadata)) => metadata.iter().find(|m| m.name == target_path).is_some(),
                _ => false,
            }
        } {
            bail!("destination already exists");
        }

        // looks promising, proceeding

        self.renterd
            .bus()
            .object()
            .rename(
                source_path.clone(),
                target_path.clone(),
                bucket.clone(),
                true,
                rename_mode,
            )
            .await?;

        let mut affected_ids = HashSet::new();
        affected_ids.insert(source.id());
        affected_ids.insert(source.parent());
        affected_ids.insert(target_dir.id());

        // update db entry
        affected_ids.extend(
            self.inode_manager
                .mv(source.id(), target_dir.id(), target_name)
                .await?,
        );

        self.cache_manager
            .invalidate_caches(affected_ids.into_iter().collect_vec());

        info!(
            "mv from /{}{} to /{}{}",
            bucket, source_path, bucket, target_path
        );

        Ok(())
    }
}

impl TryFrom<(Metadata, &str, u64)> for Inode {
    type Error = anyhow::Error;

    fn try_from(
        (metadata, path, parent_id): (Metadata, &str, u64),
    ) -> std::result::Result<Self, Self::Error> {
        let name = match metadata.name.strip_prefix(&path) {
            Some(name) => name,
            None => bail!("path does not match name"),
        };

        let (name, is_dir) = match name.strip_suffix("/") {
            Some(name) => (name.to_string(), true),
            None => (name.to_string(), false),
        };

        let last_modified = metadata.mod_time.to_utc();

        if is_dir {
            Ok(Inode::Directory(Directory::new(
                0,
                name,
                last_modified,
                parent_id,
            )))
        } else {
            Ok(Inode::File(File::new(
                0,
                name,
                metadata.size,
                last_modified,
                parent_id,
            )))
        }
    }
}

struct PendingWrites {
    data: RwLock<(
        HashMap<u64, HashSet<u64>>,
        HashMap<u64, watch::Receiver<File>>,
    )>,
}

impl PendingWrites {
    fn new() -> Self {
        Self {
            data: RwLock::new((HashMap::new(), HashMap::new())),
        }
    }

    pub fn by_name_parent(&self, name: &str, parent_id: u64) -> Option<File> {
        let pending_writes = self.data.read();
        if let Some(ids) = pending_writes.0.get(&parent_id).map(|e| e.iter()) {
            if let Some(file) = ids
                .map(|id| pending_writes.1.get(id))
                .flatten()
                .filter_map(|f| {
                    let file = f.borrow();
                    if file.name() == name {
                        Some(file.clone())
                    } else {
                        None
                    }
                })
                .next()
            {
                return Some(file);
            }
        }
        None
    }

    pub fn by_id(&self, id: u64) -> Option<File> {
        self.data.read().1.get(&id).map(|e| e.borrow().clone())
    }

    pub fn by_parent(&self, parent_id: u64) -> Vec<File> {
        let pending_writes = self.data.read();
        pending_writes
            .0
            .get(&parent_id)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| pending_writes.1.get(id).map(|e| Some(e.borrow().clone())))
                    .collect()
            })
            .flatten()
            .unwrap_or_else(|| vec![])
    }

    pub fn insert(&self, reserved_id: u64, parent_id: u64, file_rx: watch::Receiver<File>) {
        let mut pending_writes = self.data.write();
        pending_writes.1.insert(reserved_id, file_rx);
        pending_writes
            .0
            .entry(parent_id)
            .or_insert_with(|| HashSet::new())
            .insert(reserved_id);
    }

    pub fn remove(&self, reserved_id: u64, parent_id: u64) {
        let mut pending_writes = self.data.write();
        pending_writes.1.remove(&reserved_id);
        let mut keep = false;
        if let Some(e) = pending_writes.0.get_mut(&parent_id) {
            e.remove(&reserved_id);
            keep = !e.is_empty();
        };
        if !keep {
            pending_writes.0.remove(&parent_id);
        }
    }
}
