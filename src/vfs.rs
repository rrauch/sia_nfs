use crate::SqlitePool;
use anyhow::{anyhow, bail, Result};
use async_recursion::async_recursion;
use bimap::BiHashMap;
use chrono::{DateTime, Utc};
use futures::{AsyncRead, AsyncSeek};
use futures_util::io::Cursor;
use futures_util::{StreamExt, TryStreamExt};
use itertools::{Either, Itertools};
use renterd_client::Client as RenterdClient;
use sqlx::FromRow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{
    OwnedRwLockReadGuard, OwnedRwLockWriteGuard, OwnedSemaphorePermit, RwLock, Semaphore,
};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{info, instrument};

const ROOT_ID: u64 = 1;

pub(crate) struct Vfs {
    renterd: RenterdClient,
    db: SqlitePool,
    root: Directory,
    bucket_ids: BiHashMap<u64, String>,
    buckets: BTreeMap<u64, Directory>,
    file_locks: Arc<Mutex<HashMap<String, Arc<RwLock<()>>>>>,
    download_limiter: Arc<Semaphore>,
    lock_gc: JoinHandle<()>,
}

impl Drop for Vfs {
    fn drop(&mut self) {
        self.lock_gc.abort();
    }
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
        let root_id = ROOT_ID as i64;

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
                        Directory {
                            id: 0,
                            name,
                            parent: ROOT_ID,
                            last_modified: last_modified.clone(),
                        },
                    );
                    Ok(buckets)
                } else {
                    Err(anyhow!("unknown bucket: '{}', check your config", name))
                }
            })?;

        // get the highest known bucket id
        let mut bucket_id = {
            sqlx::query!(
                // sqlx seems unable to determine the type of MAX(id) without the explicit CAST statement
                "SELECT CAST(MAX(id) AS Integer) AS max_id FROM fs_entries WHERE parent = ?",
                root_id,
            )
            .fetch_one(db.read())
            .await?
            .max_id
            .map(|x| x as u64)
            .unwrap_or(0)
            .max(100)
        };
        bucket_id += 1;

        // housekeeping
        let mut tx = db.write().begin().await?;
        let _ = sqlx::query!(
            "DELETE FROM fs_entries WHERE id != ? AND parent = ? AND entry_type != 'D'",
            root_id,
            root_id
        )
        .execute(tx.as_mut())
        .await?;

        let known_buckets: Vec<String> = sqlx::query!(
            "SELECT DISTINCT(name) as name FROM fs_entries WHERE parent = ? AND id != ?",
            root_id,
            root_id,
        )
        .fetch_all(tx.as_mut())
        .await?
        .into_iter()
        .map(|r| r.name)
        .collect();

        // delete obsolete buckets
        for bucket in &known_buckets {
            if !buckets.contains_key(bucket) {
                let _ = sqlx::query!(
                    "DELETE FROM fs_entries WHERE name = ? AND parent = ?",
                    bucket,
                    root_id
                )
                .execute(tx.as_mut())
                .await?;
            }
        }

        // create missing buckets
        for bucket in buckets.keys() {
            if !known_buckets.contains(bucket) {
                let id = bucket_id as i64;
                let _ = sqlx::query!(
                    "INSERT INTO fs_entries (id, parent, name, entry_type) VALUES (?, ?, ?, 'D')",
                    id,
                    root_id,
                    bucket
                )
                .execute(tx.as_mut())
                .await?;
                bucket_id += 1;
            }
        }
        tx.commit().await?;

        // map ids to buckets
        let bucket_ids: BiHashMap<u64, String> = sqlx::query!(
            "SELECT id, name FROM fs_entries WHERE parent = ? and id != ? ORDER BY id ASC",
            root_id,
            root_id
        )
        .fetch_all(db.read())
        .await?
        .into_iter()
        .map(|r| (r.id as u64, r.name))
        .collect();

        let buckets = buckets
            .into_iter()
            .map(|(name, mut dir)| {
                let id = bucket_ids
                    .get_by_right(&name)
                    .expect("bucket is unexplainably missing")
                    .clone();
                dir.id = id;
                (id, dir)
            })
            .collect::<BTreeMap<_, _>>();

        let file_locks = Arc::new(Mutex::new(HashMap::new()));
        let lock_gc = {
            let file_locks = file_locks.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    {
                        let mut locks = file_locks.lock().expect("unable to get file locks");
                        // remove all locks currently not held outside this map
                        locks.retain(|_, lock| Arc::strong_count(lock) > 1)
                    }
                }
            })
        };

        let download_limiter = Arc::new(Semaphore::new(max_concurrent_downloads.get()));

        Ok(Self {
            renterd,
            db,
            root: Directory {
                id: ROOT_ID,
                name: "root".to_string(),
                last_modified: Utc::now(),
                parent: ROOT_ID,
            },
            buckets,
            bucket_ids,
            file_locks,
            download_limiter,
            lock_gc,
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
            let id = match self.bucket_ids.get_by_right(name) {
                Some(id) => id.clone(),
                None => return Ok(None),
            };
            return Ok(self
                .buckets
                .get(&id)
                .map(|dir| Inode::Directory(dir.clone())));
        };

        let (bucket, path) = self
            .inode_to_bucket_path(Inode::Directory(parent))
            .await?
            .ok_or(anyhow!("unable to find bucket/path for parent"))?;

        self.inode_by_bucket_path(bucket, format!("{}{}", path, name), parent_id)
            .await
    }

    async fn inode_by_bucket_path(
        &self,
        bucket: String,
        path: String,
        parent_id: u64,
    ) -> Result<Option<Inode>> {
        let (_, name) = path
            .trim_end_matches('/')
            .rsplit_once('/')
            .ok_or(anyhow!("invalid path"))?;
        if name.is_empty() {
            bail!("invalid path, name is empty");
        }

        let mut stream = self.renterd.bus().object().list(
            NonZeroUsize::new(1).unwrap(),
            Some(path.clone()),
            Some(bucket),
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
            bail!("incorrect metadata name");
        }

        let inode_type = if metadata.name.ends_with('/') {
            InodeType::Directory
        } else {
            InodeType::File
        };

        let id = if let Some(inode) = {
            let parent_id = parent_id as i64;
            let name = name.to_string();
            sqlx::query_as!(
                InodeRecord,
                "SELECT id, name, parent, entry_type FROM fs_entries WHERE parent = ? AND name = ?",
                parent_id,
                name
            )
            .fetch_optional(self.db.read())
            .await?
            .map(|r| Inode::try_from(r))
            .transpose()?
        } {
            if inode.inode_type() != inode_type {
                // the entry type has changed, delete
                let parent_id = parent_id as i64;
                let id = inode.id() as i64;
                sqlx::query!(
                    "DELETE FROM fs_entries WHERE id = ? and parent = ?",
                    id,
                    parent_id
                )
                .execute(self.db.write())
                .await?;
                None
            } else {
                Some(inode.id())
            }
        } else {
            None
        };

        let id = match id {
            Some(id) => id,
            None => {
                let inode_type = inode_type.as_ref();
                let parent_id = parent_id as i64;
                sqlx::query!(
                    "INSERT INTO fs_entries (name, parent, entry_type) VALUES (?, ?, ?)",
                    name,
                    parent_id,
                    inode_type,
                )
                .execute(self.db.write())
                .await?
                .last_insert_rowid() as u64
            }
        };

        Ok(Some(Inode::new(
            id,
            name.to_string(),
            metadata.size,
            metadata.mod_time.to_utc(),
            parent_id,
            inode_type,
        )))
    }

    pub async fn inode_by_id(&self, id: u64) -> Result<Option<Inode>> {
        Self::_inode_by_id(&self, id).await
    }

    #[async_recursion]
    async fn _inode_by_id(&self, id: u64) -> Result<Option<Inode>> {
        if id < ROOT_ID {
            return Ok(None);
        }
        if id == ROOT_ID {
            return Ok(Some(Inode::Directory(self.root.clone())));
        } else if let Some(dir) = self.buckets.get(&id) {
            return Ok(Some(Inode::Directory(dir.clone())));
        }

        let inode = match {
            let db_id = id as i64;
            sqlx::query_as!(
                InodeRecord,
                "SELECT id, parent, entry_type, name FROM fs_entries WHERE id = ?",
                db_id
            )
            .fetch_optional(self.db.read())
            .await?
            .map(|i| Inode::try_from(i))
            .transpose()
        }? {
            Some(inode) => inode,
            None => return Ok(None),
        };

        let parent_id = inode.parent();

        let (bucket, path) = match self.inode_to_bucket_path(inode).await? {
            Some((bucket, path)) => (bucket, path),
            None => return Ok(None),
        };

        self.inode_by_bucket_path(bucket, path, parent_id).await
    }

    #[async_recursion]
    async fn inode_to_bucket_path(&self, inode: Inode) -> Result<Option<(String, String)>> {
        let mut bucket = None;
        let mut components = vec![];
        let mut inode = Some(inode);
        let mut last_is_dir = false;
        let mut counter = 0usize;

        while let Some(current_inode) = inode {
            let parent_id;
            match current_inode {
                Inode::Directory(dir) => {
                    if dir.id == ROOT_ID {
                        // reached root
                        break;
                    }
                    if dir.parent == ROOT_ID {
                        // this is the bucket
                        bucket = Some(dir.name);
                        break;
                    } else {
                        if counter == 0 {
                            last_is_dir = true;
                        }
                        components.push(dir.name);
                        parent_id = dir.parent;
                    }
                }
                Inode::File(file) => {
                    assert_ne!(
                        file.parent, ROOT_ID,
                        "invalid fs, root can only contain buckets"
                    );
                    components.push(file.name);
                    parent_id = file.parent;
                }
            }
            counter += 1;
            inode = self._inode_by_id(parent_id).await?;
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
        if dir.id == ROOT_ID {
            return Ok(self
                .buckets
                .values()
                .map(|d| Inode::Directory(d.clone()))
                .collect::<Vec<_>>());
        }
        let dir_id = dir.id;
        let (bucket, path) = self
            .inode_to_bucket_path(Inode::Directory(dir.clone()))
            .await?
            .ok_or(anyhow!("invalid dir"))?;

        let _read_lock = self.read_lock(&bucket, &path).await?;

        let mut stream = match self
            .renterd
            .bus()
            .object()
            .get_stream(
                path.as_str(),
                NonZeroUsize::new(1000).unwrap(),
                None,
                Some(bucket),
            )
            .await?
            .ok_or(anyhow!("path not found"))?
        {
            Either::Left(_file) => bail!("expected a directory but got a file"),
            Either::Right(stream) => stream,
        };

        let parent = dir_id as i64;
        let mut obsolete: HashMap<_, _> = sqlx::query_as!(
            InodeRecord,
            "SELECT id, name, parent, entry_type FROM fs_entries WHERE parent = ?",
            parent
        )
        .fetch_all(self.db.read())
        .await?
        .into_iter()
        .map(|r| Inode::try_from(r).map(|i| (i.name().to_string(), i)))
        .try_collect()?;

        let mut known = vec![];
        let mut new = vec![];

        while let Some(metadata) = stream.try_next().await? {
            metadata
                .into_iter()
                .filter_map(|m| {
                    if let Some(name) = m.name.strip_prefix(&path) {
                        let (name, is_dir) = match name.strip_suffix("/") {
                            Some(name) => (name, true),
                            None => (name, false),
                        };
                        let mut inode_id = 0u64;
                        if let Some(inode) = obsolete.remove(name) {
                            let (id, was_dir) = match inode {
                                Inode::Directory(dir) => (dir.id, true),
                                Inode::File(file) => (file.id, false),
                            };
                            if is_dir != was_dir {
                                // an inode with the same name exists, however it has changed type
                                // better remove the previous entry and replace it with a new one
                                let inode_type = if was_dir {
                                    InodeType::Directory
                                } else {
                                    InodeType::File
                                };
                                obsolete.insert(
                                    name.to_string(),
                                    Inode::new(
                                        id,
                                        name.to_string(),
                                        0,
                                        Utc::now(),
                                        dir_id,
                                        inode_type,
                                    ),
                                );
                            } else {
                                // this is a known inode
                                inode_id = id;
                            }
                        }
                        let inode_type = if is_dir {
                            InodeType::Directory
                        } else {
                            InodeType::File
                        };
                        Some(Inode::new(
                            inode_id,
                            name.to_string(),
                            m.size,
                            m.mod_time.to_utc(),
                            dir_id,
                            inode_type,
                        ))
                    } else {
                        // it's invalid
                        None
                    }
                })
                .for_each(|inode| {
                    if inode.id() == 0 {
                        // this is a new inode
                        new.push(inode)
                    } else {
                        // this ia a known inode
                        known.push(inode)
                    }
                });
        }

        if !new.is_empty() || !obsolete.is_empty() {
            // something has changed here
            // update the database accordingly
            let mut tx = self.db.write().begin().await?;
            for (_, inode) in obsolete {
                let id = inode.id() as i64;
                let _ = sqlx::query!(
                    "DELETE FROM fs_entries WHERE id = ? AND parent = ?",
                    id,
                    parent
                )
                .execute(tx.as_mut())
                .await?;
            }
            for mut inode in new {
                let inode_type = inode.inode_type();
                let inode_type = inode_type.as_ref();
                let name = inode.name();
                let id = sqlx::query!(
                    "INSERT INTO fs_entries (name, parent, entry_type) VALUES (?, ?, ?)",
                    name,
                    parent,
                    inode_type,
                )
                .execute(tx.as_mut())
                .await?
                .last_insert_rowid();
                inode.set_id(id as u64);
                known.push(inode);
            }
            tx.commit().await?;
        }

        Ok(known)
    }

    async fn read_lock(&self, bucket: &str, path: &str) -> Result<OwnedRwLockReadGuard<()>> {
        let rw_lock = self._rw_lock(bucket, path);
        tracing::trace!("acquiring read lock for /{}{}", bucket, path);
        Ok(timeout(Duration::from_secs(60), rw_lock.read_owned()).await?)
    }

    async fn write_lock(&self, bucket: &str, path: &str) -> Result<OwnedRwLockWriteGuard<()>> {
        let rw_lock = self._rw_lock(bucket, path);
        tracing::trace!("acquiring write lock for /{}{}", bucket, path);
        Ok(timeout(Duration::from_secs(60), rw_lock.write_owned()).await?)
    }

    fn _rw_lock(&self, bucket: &str, path: &str) -> Arc<RwLock<()>> {
        let path = format!("/{}{}", bucket, path);
        let mut lock = self.file_locks.lock().expect("unable to lock file_locks");
        lock.entry(path)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    pub async fn read_file(&self, file: &File) -> Result<FileReader> {
        let (bucket, path) = self
            .inode_to_bucket_path(Inode::File(file.clone()))
            .await?
            .ok_or(anyhow!("invalid file"))?;

        // acquire the actual read lock
        let _read_lock = self.read_lock(&bucket, &path).await?;

        tracing::trace!("waiting for download permit");
        let _download_permit = timeout(
            Duration::from_secs(60),
            self.download_limiter.clone().acquire_owned(),
        )
        .await??;

        let object = self
            .renterd
            .worker()
            .object()
            .download(path, Some(bucket))
            .await?
            .ok_or(anyhow!("renterd couldn't find the file"))?;

        let size = object.length.ok_or(anyhow!("file size is unknown"))?;
        let stream = object.open_seekable_stream().await?;

        Ok(FileReader {
            file: file.clone(),
            offset: 0,
            size,
            stream: Box::new(stream),
            _read_lock,
            _download_permit,
        })
    }

    #[instrument(skip(self))]
    pub async fn mkdir(&self, parent: &Directory, name: String) -> Result<Directory> {
        if parent.parent == self.root.id() {
            bail!("unable to write in root directory");
        }

        let (bucket, path) = self
            .inode_to_bucket_path(Inode::Directory(parent.clone()))
            .await?
            .ok_or(anyhow!("directory not found"))?;
        // lock the parent path first
        let _write_lock = self.write_lock(&bucket, &path).await?;

        if let Some(_) = self
            .inode_by_name_parent(name.as_str(), parent.id())
            .await?
        {
            // an inode with the same name already exists in the parent directory
            bail!("inode already exists");
        }

        if name.ends_with("/") {
            bail!("invalid name");
        }

        // a directory is created by uploading an empty file with a name ending in "/"
        let path = format!("{}{}/", path, name);
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

        let _write_lock = self.write_lock(&bucket, &path).await?;

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
        let mut tx = self.db.write().begin().await?;

        // first, clean up the temp id table
        let _ = sqlx::query!("DELETE FROM deleted_fs_entries")
            .execute(tx.as_mut())
            .await?;

        // then delete the inode
        let id = inode.id() as i64;
        let _ = sqlx::query!("DELETE FROM fs_entries WHERE id = ?", id)
            .execute(tx.as_mut())
            .await?;

        // the table has a "ON DELETE CASCADE" constraint, so any child inodes have been automatically deleted
        // the ids of all deleted items will end up in the temp table (this is done by the delete trigger)
        sqlx::query!("SELECT id FROM deleted_fs_entries")
            .fetch_all(tx.as_mut())
            .await?
            .into_iter()
            .for_each(|r| {
                affected_ids.insert(r.id as u64);
            });

        // clean up the temp table again
        let _ = sqlx::query!("DELETE FROM deleted_fs_entries")
            .execute(tx.as_mut())
            .await?;

        tx.commit().await?;

        info!("deleted /{}{} ({})", bucket, path, inode.id());

        Ok(())
    }
}

pub struct FileReader {
    file: File,
    _read_lock: OwnedRwLockReadGuard<()>,
    _download_permit: OwnedSemaphorePermit,
    offset: u64,
    size: u64,
    stream: Box<dyn ReadStream + Send + Unpin>,
}

impl FileReader {
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
        self.offset == self.size
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
        if let Poll::Ready(Ok(bytes_read)) = result {
            self.offset += bytes_read as u64;
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
        if let Poll::Ready(Ok(position)) = result {
            self.offset = position;
        }
        result
    }
}

impl Drop for FileReader {
    #[instrument(skip(self), name = "file_reader_drop")]
    fn drop(&mut self) {
        tracing::debug!(
            id = self.file.id,
            name = self.file.name,
            offset = self.offset,
            "file_reader closed"
        );
    }
}

#[derive(PartialEq, Clone)]
pub enum InodeType {
    Directory,
    File,
}

impl Display for InodeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl AsRef<str> for InodeType {
    fn as_ref(&self) -> &str {
        match self {
            InodeType::Directory => "D",
            InodeType::File => "F",
        }
    }
}

impl FromStr for InodeType {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("D") {
            Ok(InodeType::Directory)
        } else if s.eq_ignore_ascii_case("F") {
            Ok(InodeType::File)
        } else {
            Err(anyhow!("Invalid inode type `{}`", s))
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub(crate) struct Directory {
    id: u64,
    name: String,
    last_modified: DateTime<Utc>,
    parent: u64,
}

impl Directory {
    pub fn id(&self) -> u64 {
        self.id
    }

    fn set_id(&mut self, id: u64) {
        self.id = id;
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        &self.last_modified
    }

    pub fn parent(&self) -> u64 {
        self.parent
    }
}

#[derive(PartialEq, Clone, Debug)]
pub(crate) struct File {
    id: u64,
    name: String,
    size: u64,
    last_modified: DateTime<Utc>,
    parent: u64,
}

impl File {
    pub fn id(&self) -> u64 {
        self.id
    }

    fn set_id(&mut self, id: u64) {
        self.id = id;
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        &self.last_modified
    }

    pub fn parent(&self) -> u64 {
        self.parent
    }
}

#[derive(PartialEq, Clone, Debug)]
pub(crate) enum Inode {
    Directory(Directory),
    File(File),
}

impl Inode {
    fn new(
        id: u64,
        name: String,
        size: u64,
        last_modified: DateTime<Utc>,
        parent: u64,
        inode_type: InodeType,
    ) -> Self {
        match inode_type {
            InodeType::Directory => Inode::Directory(Directory {
                id,
                name,
                last_modified,
                parent,
            }),
            InodeType::File => Inode::File(File {
                id,
                name,
                size,
                last_modified,
                parent,
            }),
        }
    }

    pub fn id(&self) -> u64 {
        match &self {
            Inode::File(file) => file.id(),
            Inode::Directory(dir) => dir.id(),
        }
    }

    fn set_id(&mut self, id: u64) {
        match self {
            Inode::File(file) => file.set_id(id),
            Inode::Directory(dir) => dir.set_id(id),
        }
    }

    pub fn name(&self) -> &str {
        match &self {
            Inode::File(file) => file.name(),
            Inode::Directory(dir) => dir.name(),
        }
    }

    pub fn parent(&self) -> u64 {
        match self {
            Inode::File(file) => file.parent(),
            Inode::Directory(dir) => dir.parent(),
        }
    }

    pub fn inode_type(&self) -> InodeType {
        match &self {
            Inode::File(_) => InodeType::File,
            Inode::Directory(_) => InodeType::Directory,
        }
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        match self {
            Inode::File(file) => file.last_modified(),
            Inode::Directory(dir) => dir.last_modified(),
        }
    }

    pub fn size(&self) -> Option<u64> {
        match self {
            Inode::File(file) => Some(file.size()),
            Inode::Directory(_) => None,
        }
    }
}

#[derive(FromRow)]
struct InodeRecord {
    id: i64,
    name: String,
    parent: i64,
    entry_type: String,
}

impl TryFrom<InodeRecord> for Inode {
    type Error = anyhow::Error;

    fn try_from(r: InodeRecord) -> std::result::Result<Self, Self::Error> {
        let inode_type = InodeType::from_str(r.entry_type.as_str())?;
        Ok(Inode::new(
            r.id as u64,
            r.name,
            0,
            Utc::now(),
            r.parent as u64,
            inode_type,
        ))
    }
}
