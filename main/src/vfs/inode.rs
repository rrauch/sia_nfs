use crate::vfs::ROOT_ID;
use crate::SqlitePool;
use anyhow::bail;
use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::Stream;
use futures_util::TryStreamExt;
use itertools::Itertools;
use renterd_client::bus::object::Metadata;
use sqlx::{FromRow, Sqlite, SqliteExecutor, Transaction};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::mem::discriminant;
use std::sync::Arc;
use synonym::Synonym;

const ROOT_INODE_ID: InodeId = InodeId(ROOT_ID);
const MIN_BUCKET_ID: InodeId = InodeId(100);
const MIN_OBJECT_ID: InodeId = InodeId(10_000);

#[derive(Synonym)]
pub struct InodeId(u64);

#[derive(Synonym)]
pub struct ObjectId(u64);

impl From<ObjectId> for InodeId {
    fn from(value: ObjectId) -> Self {
        InodeId(value.0)
    }
}

impl TryFrom<InodeId> for ObjectId {
    type Error = anyhow::Error;

    fn try_from(value: InodeId) -> std::result::Result<Self, Self::Error> {
        let value = value.0;
        if value < MIN_OBJECT_ID.0 {
            bail!("not an object id");
        }
        Ok(ObjectId(value))
    }
}

#[derive(PartialEq, Clone, Debug)]
pub enum Inode {
    Root(Root),
    Bucket(Bucket),
    Object(Object),
}

impl Inode {
    pub fn id(&self) -> InodeId {
        match self {
            Inode::Root(root) => root.id(),
            Inode::Bucket(bucket) => bucket.id(),
            Inode::Object(object) => object.id().into(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Inode::Root(root) => root.name(),
            Inode::Bucket(bucket) => bucket.name(),
            Inode::Object(object) => object.name(),
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Inode::Root(_) => 0,
            Inode::Bucket(_) => 0,
            Inode::Object(object) => object.size(),
        }
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        match self {
            Inode::Root(root) => root.last_modified(),
            Inode::Bucket(bucket) => bucket.last_modified(),
            Inode::Object(object) => object.last_modified(),
        }
    }

    pub fn as_root(&self) -> Option<&Root> {
        if let Inode::Root(root) = &self {
            Some(root)
        } else {
            None
        }
    }

    pub fn as_bucket(&self) -> Option<&Bucket> {
        if let Inode::Bucket(bucket) = &self {
            Some(bucket)
        } else {
            None
        }
    }

    pub fn as_object(&self) -> Option<&Object> {
        if let Inode::Object(object) = &self {
            Some(object)
        } else {
            None
        }
    }

    pub fn as_file(&self) -> Option<&File> {
        if let Inode::Object(Object::File(file)) = &self {
            Some(file)
        } else {
            None
        }
    }

    pub fn as_directory(&self) -> Option<&Directory> {
        if let Inode::Object(Object::Directory(dir)) = &self {
            Some(dir)
        } else {
            None
        }
    }

    pub fn as_parent(&self) -> Option<&dyn Parent> {
        match self {
            Inode::Bucket(bucket) => Some(bucket),
            Inode::Object(Object::Directory(dir)) => Some(dir),
            _ => None,
        }
    }

    pub fn try_into_parent(self) -> std::result::Result<Box<dyn Parent>, Self> {
        match self {
            Inode::Bucket(bucket) => Ok(Box::new(bucket)),
            Inode::Object(Object::Directory(dir)) => Ok(Box::new(dir)),
            _ => Err(self),
        }
    }
}

impl From<Root> for Inode {
    fn from(value: Root) -> Self {
        Inode::Root(value)
    }
}

impl TryFrom<Inode> for Root {
    type Error = Inode;

    fn try_from(value: Inode) -> std::result::Result<Self, Self::Error> {
        match value {
            Inode::Root(root) => Ok(root),
            _ => Err(value),
        }
    }
}

impl From<Bucket> for Inode {
    fn from(value: Bucket) -> Self {
        Inode::Bucket(value)
    }
}

impl TryFrom<Inode> for Bucket {
    type Error = Inode;

    fn try_from(value: Inode) -> std::result::Result<Self, Self::Error> {
        match value {
            Inode::Bucket(bucket) => Ok(bucket),
            _ => Err(value),
        }
    }
}

impl From<Object> for Inode {
    fn from(value: Object) -> Self {
        Inode::Object(value)
    }
}

impl TryFrom<Inode> for Object {
    type Error = Inode;

    fn try_from(value: Inode) -> std::result::Result<Self, Self::Error> {
        match value {
            Inode::Object(object) => Ok(object),
            _ => Err(value),
        }
    }
}

impl From<File> for Inode {
    fn from(value: File) -> Self {
        Inode::Object(Object::File(value))
    }
}

impl TryFrom<Inode> for File {
    type Error = Inode;

    fn try_from(value: Inode) -> std::result::Result<Self, Self::Error> {
        match value {
            Inode::Object(Object::File(file)) => Ok(file),
            _ => Err(value),
        }
    }
}

impl From<Directory> for Inode {
    fn from(value: Directory) -> Self {
        Inode::Object(Object::Directory(value))
    }
}

impl TryFrom<Inode> for Directory {
    type Error = Inode;

    fn try_from(value: Inode) -> std::result::Result<Self, Self::Error> {
        match value {
            Inode::Object(Object::Directory(dir)) => Ok(dir),
            _ => Err(value),
        }
    }
}

pub trait Parent: Send + Sync + Debug {
    fn id(&self) -> InodeId;
    fn bucket(&self) -> &Bucket;
    fn path(&self) -> &str;
    fn object_id(&self) -> Option<ObjectId>;
    fn to_inode(&self) -> Inode;
}

impl<P: Parent + ?Sized> Parent for Box<P> {
    #[inline]
    fn id(&self) -> InodeId {
        (**self).id()
    }

    #[inline]
    fn bucket(&self) -> &Bucket {
        (**self).bucket()
    }

    #[inline]
    fn path(&self) -> &str {
        (**self).path()
    }

    #[inline]
    fn object_id(&self) -> Option<ObjectId> {
        (**self).object_id()
    }

    #[inline]
    fn to_inode(&self) -> Inode {
        (**self).to_inode()
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct Root {
    last_modified: DateTime<Utc>,
}

impl Root {
    pub fn id(&self) -> InodeId {
        ROOT_ID.into()
    }

    pub fn name(&self) -> &str {
        "ROOT"
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        &self.last_modified
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct Bucket {
    id: InodeId,
    name: String,
    last_modified: DateTime<Utc>,
}

impl Bucket {
    pub fn id(&self) -> InodeId {
        self.id
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        &self.last_modified
    }
}

impl Parent for Bucket {
    fn id(&self) -> InodeId {
        self.id()
    }
    fn bucket(&self) -> &Bucket {
        &self
    }

    fn path(&self) -> &str {
        "/"
    }

    fn object_id(&self) -> Option<ObjectId> {
        None
    }

    fn to_inode(&self) -> Inode {
        self.clone().into()
    }
}

#[derive(PartialEq, Clone, Debug)]
pub enum Object {
    File(File),
    Directory(Directory),
}

impl Object {
    pub fn id(&self) -> ObjectId {
        match self {
            Object::File(file) => file.id(),
            Object::Directory(dir) => dir.id(),
        }
    }

    fn set_id(&mut self, id: ObjectId) {
        match self {
            Object::File(file) => file.set_id(id),
            Object::Directory(dir) => dir.set_id(id),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Object::File(file) => file.name(),
            Object::Directory(dir) => dir.name(),
        }
    }

    pub fn bucket(&self) -> &Bucket {
        match self {
            Object::File(file) => file.bucket(),
            Object::Directory(dir) => dir.bucket(),
        }
    }

    pub fn path(&self) -> &str {
        match self {
            Object::File(file) => file.path(),
            Object::Directory(dir) => dir.path(),
        }
    }

    pub fn parent_id(&self) -> InodeId {
        match self {
            Object::File(file) => file.parent_id(),
            Object::Directory(dir) => dir.parent_id(),
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Object::File(file) => file.size(),
            Object::Directory(dir) => dir.size(),
        }
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        match self {
            Object::File(file) => file.last_modified(),
            Object::Directory(dir) => dir.last_modified(),
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct File {
    inner: Arc<Inner>,
}

impl File {
    pub fn new(
        id: ObjectId,
        name: String,
        size: u64,
        last_modified: DateTime<Utc>,
        parent: Option<ObjectId>,
        parent_inode: InodeId,
        path: String,
        bucket: Bucket,
        etag: Option<String>,
        mime_type: Option<String>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                id,
                name,
                size,
                last_modified,
                parent_object: parent,
                parent_inode,
                path,
                bucket,
                etag,
                mime_type,
            }),
        }
    }

    pub fn id(&self) -> ObjectId {
        self.inner.id
    }

    fn set_id(&mut self, id: ObjectId) {
        Arc::make_mut(&mut self.inner).id = id;
    }

    pub fn parent_id(&self) -> InodeId {
        self.inner.parent_inode
    }

    pub fn name(&self) -> &str {
        self.inner.name.as_str()
    }

    pub fn bucket(&self) -> &Bucket {
        &self.inner.bucket
    }

    pub fn path(&self) -> &str {
        &self.inner.path
    }

    pub fn size(&self) -> u64 {
        self.inner.size
    }

    pub(super) fn set_size(&mut self, size: u64) {
        Arc::make_mut(&mut self.inner).size = size;
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        &self.inner.last_modified
    }

    pub(super) fn set_last_modified(&mut self, last_modified: DateTime<Utc>) {
        Arc::make_mut(&mut self.inner).last_modified = last_modified;
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct Directory {
    inner: Arc<Inner>,
}

impl Directory {
    pub fn id(&self) -> ObjectId {
        self.inner.id
    }

    fn set_id(&mut self, id: ObjectId) {
        Arc::make_mut(&mut self.inner).id = id;
    }

    pub fn name(&self) -> &str {
        self.inner.name.as_str()
    }

    pub fn bucket(&self) -> &Bucket {
        &self.inner.bucket
    }

    pub fn path(&self) -> &str {
        &self.inner.path
    }

    pub fn parent_id(&self) -> InodeId {
        self.inner.parent_inode
    }

    pub fn size(&self) -> u64 {
        self.inner.size
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        &self.inner.last_modified
    }
}

impl Parent for Directory {
    fn id(&self) -> InodeId {
        self.id().into()
    }

    fn bucket(&self) -> &Bucket {
        self.bucket()
    }

    fn path(&self) -> &str {
        self.path()
    }

    fn object_id(&self) -> Option<ObjectId> {
        Some(self.id())
    }

    fn to_inode(&self) -> Inode {
        self.clone().into()
    }
}

#[derive(PartialEq, Clone, Debug)]
struct Inner {
    id: ObjectId,
    name: String,
    size: u64,
    last_modified: DateTime<Utc>,
    parent_object: Option<ObjectId>,
    parent_inode: InodeId,
    path: String,
    bucket: Bucket,
    etag: Option<String>,
    mime_type: Option<String>,
}

pub(super) struct InodeManager {
    db: SqlitePool,
    buckets: HashMap<InodeId, Bucket>,
    root: Root,
}

impl InodeManager {
    pub async fn new(db: SqlitePool, buckets: &Vec<String>) -> Result<Self> {
        let mut to_delete = vec![];
        let mut missing: HashSet<&String> = buckets.into_iter().collect();
        let mut buckets = HashMap::new();
        let mut tx = db.write().begin().await?;
        for (id, name) in sqlx::query!("SELECT id, name FROM buckets")
            .fetch_all(tx.as_mut())
            .await?
            .into_iter()
            .map(|r| (InodeId::from(r.id as u64), r.name))
        {
            if let Some(_) = missing.take(&name) {
                buckets.insert(
                    id,
                    Bucket {
                        id,
                        name,
                        last_modified: Utc::now(),
                    },
                );
            } else {
                to_delete.push(id);
            }
        }

        for name in missing {
            let id = (sqlx::query!("INSERT INTO buckets (name) VALUES (?)", name)
                .execute(tx.as_mut())
                .await?
                .last_insert_rowid() as u64)
                .into();
            buckets.insert(
                id,
                Bucket {
                    id,
                    name: name.to_string(),
                    last_modified: Utc::now(),
                },
            );
            tracing::debug!(id = %id, bucket = name, "new bucket created");
        }

        for id in to_delete {
            let id = id.value() as i64;
            let rows_affected = sqlx::query!("DELETE FROM buckets WHERE id = ?", id)
                .execute(tx.as_mut())
                .await?
                .rows_affected();
            tracing::debug!(
                id = id,
                rows_affected = rows_affected,
                "obsolete bucket deleted"
            );
        }

        clear_affected_object_ids(&mut tx).await?;

        tx.commit().await?;

        Ok(Self {
            db,
            buckets,
            root: Root {
                last_modified: Utc::now(),
            },
        })
    }

    pub fn root(&self) -> Inode {
        Inode::Root(self.root.clone())
    }

    pub async fn inode_by_id(&self, id: InodeId) -> Result<Option<Inode>> {
        if id == ROOT_INODE_ID {
            return Ok(Some(Inode::Root(self.root.clone())));
        }

        if id < MIN_BUCKET_ID {
            bail!("inode id ({}) in reserved range, do not use!", id);
        }

        if id >= MIN_BUCKET_ID && id < MIN_OBJECT_ID {
            // bucket range
            return Ok(self.buckets.get(&id).map(|b| Inode::Bucket(b.clone())));
        }

        let id = id.value() as i64;
        Ok(sqlx::query_as!(
                ObjectRecord,
                "SELECT id, inode_type, name, size, last_modified, parent, path, bucket, etag, mime_type FROM objects WHERE id = ?",
                id
            ).fetch_optional(self.db.read()).await?.map(|r| {
            let bucket_id = (r.bucket as u64).into();
            let bucket = self.buckets.get(&bucket_id).ok_or(anyhow::anyhow!("no known bucket with id: {}", bucket_id))?.clone();
            Object::try_from((r, bucket))
        }).transpose()?.map(|o| o.into()))
    }

    pub async fn reserve_id(&self) -> Result<ObjectId> {
        Ok(sqlx::query!(
            "UPDATE sqlite_sequence
                SET seq = seq + 1
                WHERE name = 'objects';
            SELECT seq FROM sqlite_sequence WHERE name = 'objects';
            "
        )
        .fetch_one(self.db.write())
        .await
        .map(|r| r.seq.unwrap() as u64)?
        .into())
    }

    pub async fn new_file<P: Parent + ?Sized>(
        &self,
        reserved_id: ObjectId,
        name: String,
        size: u64,
        last_modified: DateTime<Utc>,
        parent: &P,
    ) -> Result<()> {
        let id = reserved_id.value() as i64;
        let size = size as i64;
        let parent_id = parent.object_id().map(|id| id.value() as i64);
        let bucket_id = parent.bucket().id().value() as i64;

        let mut tx = self.db.write().begin().await?;

        let _ = sqlx::query!(
            "INSERT INTO objects (id, inode_type, name, size, last_modified, parent, bucket) VALUES (?, 'F', ?, ?, ?, ?, ?)",
            id,
            name,
            size,
            last_modified,
            parent_id,
            bucket_id
        )
            .execute(tx.as_mut())
            .await?;

        self.reset_last_sync(&vec![parent.id()], &mut tx).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn delete(&self, object: &Object) -> Result<Vec<ObjectId>> {
        let mut affected_ids = HashSet::new();
        affected_ids.insert(object.id());
        let mut tx = self.db.write().begin().await?;

        clear_affected_object_ids(&mut tx).await?;

        let id = object.id().value() as i64;
        let _ = sqlx::query!("DELETE FROM objects WHERE id = ?", id)
            .execute(tx.as_mut())
            .await?;

        self.reset_last_sync(&vec![object.parent_id()], &mut tx)
            .await?;

        collect_affected_object_ids(&mut tx, &mut affected_ids).await?;

        tx.commit().await?;

        Ok(affected_ids.into_iter().collect_vec())
    }

    pub async fn mv<P: Parent + ?Sized>(
        &self,
        object: &Object,
        target_parent: &P,
        target_name: Option<&str>,
    ) -> Result<Vec<ObjectId>> {
        if target_name.is_none()
            && target_parent.object_id().is_some()
            && target_parent.object_id().unwrap() == object.id()
        {
            // nothing to do
            return Ok(vec![]);
        }

        if target_parent.bucket().id() != object.bucket().id() {
            bail!("cannot move object across buckets");
        }

        let mut affected_ids = HashSet::new();
        affected_ids.insert(object.id());

        let id = object.id().value() as i64;
        let mut tx = self.db.write().begin().await?;

        clear_affected_object_ids(&mut tx).await?;

        let parent = target_parent.object_id().map(|id| id.value() as i64);
        let _ = match target_name {
            Some(target_name) => {
                sqlx::query!(
                    "UPDATE objects SET name = ?, parent = ? WHERE id = ?",
                    target_name,
                    parent,
                    id
                )
                .execute(tx.as_mut())
                .await?
            }
            None => {
                sqlx::query!("UPDATE objects SET parent = ? WHERE id = ?", parent, id)
                    .execute(tx.as_mut())
                    .await?
            }
        };

        self.reset_last_sync(&vec![object.parent_id(), target_parent.id()], &mut tx)
            .await?;

        collect_affected_object_ids(&mut tx, &mut affected_ids).await?;

        tx.commit().await?;

        Ok(affected_ids.into_iter().collect_vec())
    }

    pub fn buckets(&self) -> impl Iterator<Item = &Bucket> {
        self.buckets.values()
    }

    pub async fn last_sync<P: Parent + ?Sized>(&self, parent: &P) -> Result<Option<DateTime<Utc>>> {
        if let Some(parent_id) = parent.object_id().map(|o| o.value() as i64) {
            // a regular directory
            Ok(
                sqlx::query!("SELECT last_sync FROM objects WHERE id = ?", parent_id,)
                    .fetch_optional(self.db.read())
                    .await?
                    .map(|r| r.last_sync.and_utc()),
            )
        } else {
            // bucket root
            let bucket_id = parent.bucket().id().value() as i64;
            Ok(
                sqlx::query!("SELECT last_sync FROM buckets WHERE id = ?", bucket_id,)
                    .fetch_optional(self.db.read())
                    .await?
                    .map(|r| r.last_sync.and_utc()),
            )
        }
    }

    async fn reset_last_sync(
        &self,
        ids: &Vec<InodeId>,
        tx: &mut Transaction<'_, Sqlite>,
    ) -> Result<usize> {
        let mut affected = 0;
        for inode_id in ids {
            let id = inode_id.value() as i64;
            if inode_id < &MIN_OBJECT_ID {
                // bucket
                affected += sqlx::query!(
                    "UPDATE buckets SET last_sync = '1970-01-01 00:00:00' WHERE id = ?",
                    id,
                )
                .execute(tx.as_mut())
                .await?
                .rows_affected() as usize;
            } else {
                // regular object
                affected += sqlx::query!(
                    "UPDATE objects SET last_sync = '1970-01-01 00:00:00' WHERE id = ?",
                    id,
                )
                .execute(tx.as_mut())
                .await?
                .rows_affected() as usize;
            }
        }
        Ok(affected)
    }

    pub async fn read_dir<P: Parent + ?Sized>(&self, parent: &P) -> Result<Vec<Object>> {
        self._read_dir(parent, self.db.read()).await
    }

    async fn _read_dir<'a, P: Parent + ?Sized>(
        &self,
        parent: &P,
        executor: impl SqliteExecutor<'a>,
    ) -> Result<Vec<Object>> {
        let bucket_id = parent.bucket().id().value() as i64;
        let parent_id = parent.object_id().map(|oid| oid.value() as i64);

        Ok(sqlx::query_as!(
            ObjectRecord,
            "SELECT id, inode_type, name, size, last_modified, parent, path, bucket, etag, mime_type FROM objects WHERE bucket = ? AND (parent IS ? OR parent = ?)",
            bucket_id,
            parent_id,
            parent_id,
        )
               .fetch_all(executor)
               .await?
               .into_iter()
               .map(|r| Object::try_from((r, parent.bucket().clone())))
               .try_collect()?)
    }

    pub async fn sync_dir<P: Parent + ?Sized>(
        &self,
        parent: &P,
        mut metadata: impl Stream<Item = Result<Vec<Metadata>>> + Unpin,
    ) -> Result<(Vec<Object>, Vec<ObjectId>)> {
        let bucket_id = parent.bucket().id().value() as i64;
        let parent_id = parent.object_id().map(|oid| oid.value() as i64);

        let mut tx = self.db.write().begin().await?;
        clear_affected_object_ids(&mut tx).await?;

        let mut obsolete: HashMap<_, _> = self
            ._read_dir(parent, tx.as_mut())
            .await?
            .into_iter()
            .map(|o| (o.name().to_string(), o))
            .collect();

        let mut changed = vec![];
        let mut new = vec![];
        let mut affected_ids = HashSet::new();

        while let Some(metadata) = metadata.try_next().await? {
            for metadata in metadata {
                let mut object = Object::try_from((metadata, parent))?;
                if let Some(prev_object) = obsolete.remove(object.name()) {
                    // known object
                    object.set_id(prev_object.id());
                    if prev_object != object {
                        // this object has changed since last sync
                        changed.push((object, prev_object));
                    }
                } else {
                    // new object
                    new.push(object);
                }
            }
        }

        // delete all obsolete objects first
        for (_, object) in obsolete {
            let id = object.id().value() as i64;
            sqlx::query!("DELETE FROM objects WHERE id = ?", id,)
                .execute(tx.as_mut())
                .await?;
        }

        // update all changed objects
        for (object, prev_object) in changed {
            let id = object.id().value() as i64;
            let inode_type = match object {
                Object::File(_) => "F",
                Object::Directory(_) => "D",
            };
            let name = object.name();
            let size = object.size() as i64;
            let last_modified = object.last_modified();

            if discriminant(&prev_object) != discriminant(&object) {
                // the type has changed, delete and reinsert
                sqlx::query!("DELETE FROM objects WHERE id = ?", id)
                    .execute(tx.as_mut())
                    .await?;

                sqlx::query!(
                    "INSERT INTO objects (id, name, inode_type, size, last_modified, parent, bucket) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    id,
                    name,
                    inode_type,
                    size,
                    last_modified,
                    parent_id,
                    bucket_id,
                )
                    .execute(tx.as_mut())
                    .await?;
            } else {
                // regular change, a simple update is sufficient
                sqlx::query!(
                    "UPDATE objects SET size = ?, last_modified = ? WHERE id = ?",
                    size,
                    last_modified,
                    id,
                )
                .execute(tx.as_mut())
                .await?;
            }

            affected_ids.insert(object.id());
        }

        // insert new objects
        for object in new {
            let inode_type = match object {
                Object::File(_) => "F",
                Object::Directory(_) => "D",
            };

            let name = object.name();
            let size = object.size() as i64;
            let last_modified = object.last_modified();

            let id = sqlx::query!(
                "INSERT INTO objects (name, inode_type, size, last_modified, parent, bucket) VALUES (?, ?, ?, ?, ?, ?)",
                    name,
                    inode_type,
                    size,
                    last_modified,
                    parent_id,
                    bucket_id,
                )
                .execute(tx.as_mut())
                .await?
                .last_insert_rowid();

            affected_ids.insert(ObjectId::from(id as u64));
        }

        // update last_sync
        if let Some(parent_id) = parent_id {
            // regular directory
            sqlx::query!(
                "UPDATE objects SET last_sync = CURRENT_TIMESTAMP WHERE id = ?",
                parent_id,
            )
            .execute(tx.as_mut())
            .await?;
        } else {
            // bucket root
            sqlx::query!(
                "UPDATE buckets SET last_sync = CURRENT_TIMESTAMP WHERE id = ?",
                bucket_id,
            )
            .execute(tx.as_mut())
            .await?;
        }

        collect_affected_object_ids(&mut tx, &mut affected_ids).await?;

        let objects = self._read_dir(parent, tx.as_mut()).await?;

        tx.commit().await?;

        Ok((objects, affected_ids.into_iter().collect()))
    }
}

async fn clear_affected_object_ids(tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
    sqlx::query!("DELETE FROM affected_objects")
        .execute(tx.as_mut())
        .await?;
    Ok(())
}

async fn collect_affected_object_ids(
    tx: &mut Transaction<'_, Sqlite>,
    affected_ids: &mut HashSet<ObjectId>,
) -> Result<()> {
    sqlx::query!("SELECT id FROM affected_objects")
        .fetch_all(tx.as_mut())
        .await?
        .into_iter()
        .for_each(|r| {
            affected_ids.insert((r.id as u64).into());
        });

    let _ = sqlx::query!("DELETE FROM affected_objects")
        .execute(tx.as_mut())
        .await?;

    Ok(())
}

#[derive(FromRow)]
struct ObjectRecord {
    id: i64,
    name: String,
    inode_type: String,
    size: i64,
    last_modified: NaiveDateTime,
    parent: Option<i64>,
    bucket: i64,
    path: String,
    etag: Option<String>,
    mime_type: Option<String>,
}

impl<P> TryFrom<(Metadata, &P)> for Object
where
    P: Parent + ?Sized,
{
    type Error = anyhow::Error;

    fn try_from((metadata, parent): (Metadata, &P)) -> std::result::Result<Self, Self::Error> {
        let name = match metadata.name.strip_prefix(parent.path()) {
            Some(name) => name,
            None => bail!("path does not match name"),
        };

        let (name, is_dir) = match name.split_once('/') {
            None => (name, false),
            Some((name, suffix)) if suffix.is_empty() && !name.is_empty() => (name, true),
            _ => {
                bail!("invalid name: {}", name);
            }
        };

        let last_modified = metadata.mod_time.to_utc();

        let inner = Arc::new(Inner {
            id: 0.into(),
            name: name.to_string(),
            size: metadata.size,
            last_modified,
            parent_object: parent.object_id(),
            parent_inode: parent.id(),
            path: metadata.name,
            bucket: parent.bucket().clone(),
            etag: metadata.etag,
            mime_type: metadata.mime_type,
        });

        Ok(if is_dir {
            Object::Directory(Directory { inner })
        } else {
            Object::File(File { inner })
        })
    }
}

impl TryFrom<(ObjectRecord, Bucket)> for Object {
    type Error = anyhow::Error;
    fn try_from((r, bucket): (ObjectRecord, Bucket)) -> Result<Self> {
        let parent_object = r.parent.map(|p| (p as u64).into());
        let parent_inode = parent_object
            .map(|id: ObjectId| id.into())
            .unwrap_or_else(|| bucket.id());

        let inner = Arc::new(Inner {
            id: (r.id as u64).into(),
            name: r.name,
            bucket,
            path: r.path,
            parent_object,
            parent_inode,
            last_modified: r.last_modified.and_utc(),
            size: r.size as u64,
            etag: r.etag,
            mime_type: r.mime_type,
        });

        Ok(match r.inode_type.as_str() {
            "F" => Object::File(File { inner }),
            "D" => Object::Directory(Directory { inner }),
            _ => {
                bail!("invalid inode_type: {}", r.inode_type);
            }
        })
    }
}
