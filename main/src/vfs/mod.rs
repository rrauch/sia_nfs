mod cache;
mod file_reader;
pub(crate) mod file_writer;
pub(crate) mod inode;
pub(crate) mod locking;

use crate::vfs::cache::CacheManager;
use crate::vfs::file_reader::{FileReader, FileReaderManager};
use crate::vfs::file_writer::FileWriter;
use crate::vfs::inode::{Directory, File, Inode, InodeId, InodeManager, Object, ObjectId, Parent};
use crate::vfs::locking::{LockHolder, LockManager, LockRequest};
use crate::SqlitePool;
use anyhow::{anyhow, bail, Result};
use cachalot::Cachalot;
use chrono::Utc;
use futures::pin_mut;
use futures_util::io::Cursor;
use futures_util::{StreamExt, TryStreamExt};
use itertools::{Either, Itertools};
use parking_lot::RwLock;
use renterd_client::bus::object::RenameMode;
use renterd_client::Client as RenterdClient;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{debug, instrument};

const ROOT_ID: u64 = 1;

pub(crate) struct Vfs {
    renterd: RenterdClient,
    inode_manager: InodeManager,
    lock_manager: LockManager,
    cache_manager: CacheManager,
    pending_writes: Arc<PendingWrites>,
    file_reader_manager: FileReaderManager,
    max_dir_sync_age: Duration,
}

impl Vfs {
    pub(super) async fn new(
        renterd: RenterdClient,
        db: SqlitePool,
        cachalot: Cachalot,
        buckets: &Vec<String>,
        max_concurrent_downloads_per_file: NonZeroUsize,
        max_skip_ahead: NonZeroU64,
        max_dir_sync_age: Duration,
    ) -> Result<Self> {
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

        for bucket in buckets.iter() {
            if !available_buckets.contains_key(bucket) {
                bail!("unknown bucket: '{}', check your config", bucket)
            }
        }

        let inode_manager = InodeManager::new(db, buckets).await?;
        let file_reader_manager = FileReaderManager::new(
            renterd.clone(),
            cachalot,
            max_concurrent_downloads_per_file,
            max_skip_ahead,
        )?;

        Ok(Self {
            renterd,
            inode_manager,
            lock_manager: LockManager::new(Duration::from_secs(30)),
            cache_manager: CacheManager::new(
                10_000,
                100_000,
                Duration::from_secs(60),
                Duration::from_secs(30),
                Duration::from_secs(10),
            ),
            pending_writes: Arc::new(PendingWrites::new()),
            file_reader_manager,
            max_dir_sync_age,
        })
    }

    pub fn root(&self) -> Inode {
        self.inode_manager.root()
    }

    #[instrument(skip(self))]
    pub async fn inode_by_id(&self, id: InodeId) -> Result<Option<Inode>> {
        // first, check for pending writes
        if let Some(file) = self.pending_writes.by_id(id.value()) {
            return Ok(Some(Inode::Object(Object::File(file))));
        }

        self.cache_manager
            .inode_by_id(id, self.inode_manager.inode_by_id(id))
            .await
    }

    pub async fn inode_by_name_parent<S: AsRef<str>>(
        &self,
        name: S,
        parent: &Inode,
    ) -> Result<Option<Inode>> {
        let name = name.as_ref();

        // check pending writes first
        if let Some(file) = self
            .pending_writes
            .by_name_parent(name, parent.id().value())
        {
            return Ok(Some(Inode::Object(Object::File(file))));
        }

        Ok(self
            .read_dir(parent)
            .await?
            .into_iter()
            .find(|i| i.name() == name))
    }

    #[instrument(skip(self))]
    pub async fn read_file(&self, file: &File) -> Result<FileReader> {
        let bucket = file.bucket().name().to_string();
        let path = file.path().to_string();

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

        Ok(self
            .file_reader_manager
            .open_reader(bucket, path, file.size(), read_lock)
            .await?)
    }

    #[instrument(skip(self))]
    pub async fn write_file<P: Parent + ?Sized>(
        self: &Arc<Self>,
        parent: &P,
        name: String,
    ) -> Result<FileWriter> {
        if name.contains("/") {
            bail!("invalid name");
        }

        if let Some(_) = self
            .inode_by_name_parent(name.as_str(), &parent.to_inode())
            .await?
        {
            // an inode with the same name already exists in the parent directory
            bail!("inode already exists");
        }

        let path = format!("{}{}", parent.path(), name);

        // lock the paths first
        let locks = self
            .lock_manager
            .lock([
                LockRequest::read(parent.bucket().name(), parent.path()),
                LockRequest::write(parent.bucket().name(), &path),
            ])
            .await?;

        let reserved_id = self.inode_manager.reserve_id().await?;

        let file_tx = {
            let (file_tx, file_rx) = watch::channel(File::new(
                reserved_id,
                name.clone(),
                0,
                Utc::now(),
                parent.object_id(),
                parent.id(),
                path.clone(),
                parent.bucket().clone(),
                None,
                None,
            ));

            self.pending_writes
                .insert(reserved_id.value(), parent.id().value(), file_rx);
            file_tx
        };

        let (tx, rx) = tokio::io::duplex(8192);
        let rx = rx.compat();

        let upload_task = {
            let vfs = self.clone();
            let bucket = parent.bucket().name().to_string();
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
            parent,
            path,
            self.clone(),
            locks,
            upload_task,
            tx,
            file_tx,
            self.pending_writes.clone(),
        ))
    }

    #[instrument(skip(self))]
    pub async fn mkdir<P: Parent + ?Sized>(&self, parent: &P, name: String) -> Result<Directory> {
        if name.contains("/") {
            bail!("invalid name");
        }
        let path = format!("{}{}/", parent.path(), name);

        // lock the paths first
        let _locks = self
            .lock_manager
            .lock([
                LockRequest::read(parent.bucket().name(), parent.path()),
                LockRequest::write(parent.bucket().name(), &path),
            ])
            .await?;

        let parent_inode = parent.to_inode();

        if let Some(_) = self
            .inode_by_name_parent(name.as_str(), &parent_inode)
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
                Some(parent.bucket().name().to_string()),
                Cursor::new(vec![]),
            )
            .await?;

        // looking good
        let (_, affected_ids) = self.sync_dir(parent).await?;
        let affected_ids = affected_ids
            .into_iter()
            .map(|o| InodeId::from(o))
            .merge(vec![parent.id()].into_iter())
            .unique()
            .collect_vec();
        self.cache_manager.invalidate_caches(&affected_ids).await;

        match self.inode_by_name_parent(name, &parent_inode).await? {
            Some(Inode::Object(Object::Directory(dir))) => {
                debug!(
                    "created directory /{}{} ({})",
                    parent.bucket().name(),
                    path,
                    dir.id()
                );
                Ok(dir)
            }
            _ => Err(anyhow!("directory creation failed")),
        }
    }

    #[instrument(skip(self))]
    pub async fn rm(&self, object: &Object) -> Result<()> {
        let parent = self
            .inode_by_id(object.parent_id())
            .await?
            .map(|i| i.try_into_parent())
            .ok_or(anyhow!("parent not found"))?
            .map_err(|_| anyhow!("parent invalid"))?;

        let _locks = self
            .lock_manager
            .lock([
                LockRequest::write(object.bucket().name(), object.path()),
                LockRequest::read(object.bucket().name(), parent.path()),
            ])
            .await?;

        let inode: Inode = object.clone().into();

        // do a live-lookup first to make sure the local state is accurate
        match self
            .renterd
            .bus()
            .object()
            .get_stream(
                object.path(),
                NonZeroUsize::new(10).unwrap(),
                None,
                Some(object.bucket().name().to_string()),
            )
            .await?
            .ok_or(anyhow!("path not found"))?
        {
            Either::Left(_) => {
                if inode.as_directory().is_some() {
                    //todo
                    bail!("unexpected type, expected directory but found file");
                }
            }
            Either::Right(stream) => {
                if inode.as_file().is_some() {
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
            .delete(
                object.path(),
                Some(object.bucket().name().to_string()),
                false,
            )
            .await?;

        // track all affected ids by this deletion
        let mut affected_ids = HashSet::new();
        affected_ids.insert(object.parent_id());
        affected_ids.insert(object.id().into());

        // update the database
        affected_ids.extend(
            self.inode_manager
                .delete(object)
                .await?
                .into_iter()
                .map(|oid| InodeId::from(oid)),
        );

        let affected_ids = affected_ids.into_iter().unique().collect_vec();
        self.cache_manager.invalidate_caches(&affected_ids).await;

        debug!(
            "deleted /{}{} ({})",
            object.bucket().name(),
            object.path(),
            object.id()
        );

        Ok(())
    }

    pub async fn mv<P: Parent + ?Sized>(
        &self,
        source: &Object,
        target_parent: &P,
        target_name: Option<String>,
    ) -> Result<()> {
        if source.bucket().id() != target_parent.bucket().id() {
            bail!("cannot move across buckets");
        }

        if source.parent_id() == target_parent.id() {
            if target_name.is_none() || target_name.as_ref().unwrap() == source.name() {
                // nothing to do here
                return Ok(());
            }
        }

        if InodeId::from(source.id()) == target_parent.id() {
            bail!("cannot move into self");
        }

        let source_parent = self
            .inode_by_id(source.parent_id())
            .await?
            .ok_or(anyhow!("cannot find source parent"))?
            .try_into_parent()
            .map_err(|_| anyhow!("invalid source parent"))?;

        let target_name = target_name.unwrap_or(source.name().to_string());

        let (rename_mode, tail) = match source {
            Object::File(_) => (RenameMode::Single, ""),
            Object::Directory(_) => (RenameMode::Multi, "/"),
        };

        let target_path = format!("{}{}{}", target_parent.path(), target_name, tail);

        let bucket_name = source.bucket().name();

        let _locks = self
            .lock_manager
            .lock([
                LockRequest::read(bucket_name, source_parent.path()),
                LockRequest::write(bucket_name, source.path()),
                LockRequest::read(bucket_name, target_parent.path()),
                LockRequest::write(bucket_name, target_path.as_str()),
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
                    Some(source.path().to_string()),
                    Some(bucket_name.to_string()),
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
                    Some(bucket_name.to_string()),
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
                source.path().to_string(),
                target_path.clone(),
                bucket_name.to_string(),
                true,
                rename_mode,
            )
            .await?;

        let mut affected_ids = HashSet::new();
        affected_ids.insert(source.id().into());
        affected_ids.insert(source.parent_id());
        affected_ids.insert(target_parent.id());

        // update db entry
        affected_ids.extend(
            self.inode_manager
                .mv(source, target_parent, Some(target_name.as_str()))
                .await?
                .into_iter()
                .map(|oid| InodeId::from(oid)),
        );
        let affected_ids = affected_ids.into_iter().unique().collect_vec();
        self.cache_manager.invalidate_caches(&affected_ids).await;

        debug!(
            "mv from /{}{} to /{}{}",
            bucket_name,
            source.path(),
            bucket_name,
            target_path
        );

        Ok(())
    }

    pub async fn read_dir(&self, dir: &Inode) -> Result<Vec<Inode>> {
        let dir = match dir {
            Inode::Root(_) => {
                return Ok(self
                    .inode_manager
                    .buckets()
                    .map(|b| b.to_inode())
                    .collect::<Vec<_>>());
            }
            inode => inode.as_parent().ok_or(anyhow!("not a directory"))?,
        };

        let _locks = self
            .lock_manager
            .lock([LockRequest::read(dir.bucket().name(), dir.path())])
            .await?;

        let mut inodes = self
            .cache_manager
            .read_dir(dir.id(), self._read_dir(dir))
            .await?;

        // overlay any pending writes
        inodes.extend(
            self.pending_writes
                .by_parent(dir.id().value())
                .into_iter()
                .map(|f| f.into()),
        );

        Ok(inodes)
    }

    pub async fn _read_dir<P: Parent + ?Sized>(
        &self,
        dir: &P,
    ) -> Result<(Vec<Inode>, Vec<InodeId>)> {
        let sync = if let Some(last_sync) = self
            .inode_manager
            .last_sync(dir)
            .await?
            .map(|dt| SystemTime::from(dt))
        {
            let deadline = SystemTime::now() - self.max_dir_sync_age;
            last_sync <= deadline
        } else {
            // no last sync known
            true
        };
        if sync {
            // resync the directory with renterd
            let (objects, affected_ids) = self.sync_dir(dir).await?;
            let mut affected_ids = affected_ids
                .into_iter()
                .map(|oid| InodeId::from(oid))
                .collect_vec();
            if !affected_ids.is_empty() {
                affected_ids.push(dir.id());
            }
            let affected_ids = affected_ids.into_iter().unique().collect_vec();
            Ok((
                objects.into_iter().map(|o| Inode::from(o)).collect_vec(),
                affected_ids,
            ))
        } else {
            // read directory from db
            Ok((
                self.inode_manager
                    .read_dir(dir)
                    .await?
                    .into_iter()
                    .map(|o| Inode::from(o))
                    .collect(),
                vec![],
            ))
        }
    }

    async fn sync_dir<P: Parent + ?Sized>(&self, dir: &P) -> Result<(Vec<Object>, Vec<ObjectId>)> {
        let stream = self.renterd.bus().object().list(
            NonZeroUsize::new(1000).unwrap(),
            Some(dir.path().to_string()),
            Some(dir.bucket().name().to_string()),
        )?;

        let stream = stream
            .try_filter_map(|m| async {
                Ok(Some(
                    m.into_iter()
                        .filter(|m| {
                            // filter out all entries that do not belong to this directory
                            match m.name.as_str().strip_prefix(dir.path()) {
                                Some(name) if !name.is_empty() => match name.split_once('/') {
                                    Some((name, suffix))
                                        if !suffix.is_empty() && !name.is_empty() =>
                                    {
                                        false
                                    }
                                    _ => true,
                                },
                                _ => false,
                            }
                        })
                        .collect_vec(),
                ))
            })
            .map_err(|e| e.into());

        pin_mut!(stream);

        self.inode_manager.sync_dir(dir, stream).await
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
