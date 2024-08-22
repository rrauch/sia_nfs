use crate::SqlitePool;
use anyhow::anyhow;
use anyhow::Result;
use bimap::BiHashMap;
use chrono::{DateTime, Utc};
use futures::Stream;
use futures_util::TryStreamExt;
use itertools::Itertools;
use sqlx::FromRow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

pub(super) struct InodeManager {
    db: SqlitePool,
    bucket_ids: BiHashMap<u64, String>,
    buckets: BTreeMap<u64, Directory>,
}

impl InodeManager {
    pub async fn new(
        db: SqlitePool,
        root_id: u64,
        buckets: BTreeMap<String, Directory>,
    ) -> Result<Self> {
        let root_id = root_id as i64;
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
                dir.set_id(id);
                (id, dir)
            })
            .collect::<BTreeMap<_, _>>();

        Ok(Self {
            db,
            buckets,
            bucket_ids,
        })
    }

    pub fn bucket_by_id(&self, id: u64) -> Option<&Directory> {
        self.buckets.get(&id)
    }

    pub fn bucket_by_name(&self, name: &str) -> Option<&Directory> {
        self.bucket_ids
            .get_by_right(name)
            .map(|id| self.buckets.get(id))
            .flatten()
    }

    pub fn buckets(&self) -> impl Iterator<Item = &Directory> {
        self.buckets.values()
    }

    pub async fn sync_by_name_parent(
        &self,
        name: &str,
        parent_id: u64,
        inode_type: InodeType,
        size: u64,
        last_modified: DateTime<Utc>,
    ) -> Result<Inode> {
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

        Ok(Inode::new(
            id,
            name.to_string(),
            size,
            last_modified,
            parent_id,
            inode_type,
        ))
    }

    pub async fn inode_by_id(&self, id: u64) -> Result<Option<Inode>> {
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
    }

    pub async fn sync_dir(
        &self,
        dir_id: u64,
        mut inodes: impl Stream<Item = Result<Vec<Inode>>> + Unpin,
    ) -> Result<(Vec<Inode>, Vec<u64>)> {
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

        while let Some(inodes) = inodes.try_next().await? {
            for mut inode in inodes {
                if let Some(prev_inode) = obsolete.remove(inode.name()) {
                    if prev_inode.inode_type() != inode.inode_type() {
                        // an inode with the same name exists, however it has changed type
                        // better remove the previous entry and replace it with a new one
                        obsolete.insert(inode.name().to_string(), prev_inode);
                    } else {
                        // this is a known inode
                        inode.set_id(prev_inode.id());
                    }
                }

                if inode.id() == 0 {
                    // this is a new inode
                    new.push(inode)
                } else {
                    // this ia a known inode
                    known.push(inode)
                }
            }
        }

        if !new.is_empty() || !obsolete.is_empty() {
            // something has changed here
            // update the database accordingly
            let mut tx = self.db.write().begin().await?;
            for (_, inode) in &obsolete {
                let id = inode.id() as i64;
                let _ = sqlx::query!(
                    "DELETE FROM fs_entries WHERE id = ? AND parent = ?",
                    id,
                    parent
                )
                .execute(tx.as_mut())
                .await?;
            }
            for inode in &mut new {
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
            }
            tx.commit().await?;
        }

        known.extend(new);

        Ok((
            known,
            obsolete.values().into_iter().map(|i| i.id()).collect(),
        ))
    }

    pub async fn reserve_id(&self, name: &str, parent_id: u64) -> Result<u64> {
        // Ok, this is a bit of a weird part: we need to know the id of the future file before it's even uploaded!
        // To achieve this, we do a dummy insert into the `fs_entries` table to get an id, then delete it right away
        // in the same tx. This is a bit of a hack to effectively reserve an id. Later we do a manual insert using
        // the id we get now.
        let mut tx = self.db.write().begin().await?;
        let parent_id = parent_id as i64;
        let name = name.to_string();
        let id = sqlx::query!(
            "INSERT INTO fs_entries (name, parent, entry_type) VALUES (?, ?, 'F')",
            name,
            parent_id,
        )
        .execute(tx.as_mut())
        .await?
        .last_insert_rowid() as u64;
        {
            let id = id as i64;
            let _ = sqlx::query!("DELETE FROM fs_entries where id = ?", id)
                .execute(tx.as_mut())
                .await?;
        }
        tx.commit().await?;
        Ok(id)
    }

    pub async fn new_file(&self, reserved_id: u64, name: String, parent_id: u64) -> Result<()> {
        let id = reserved_id as i64;
        let parent_id = parent_id as i64;
        let _ = sqlx::query!(
            "INSERT INTO fs_entries (id, name, parent, entry_type) VALUES (?, ?, ?, 'F')",
            id,
            name,
            parent_id,
        )
        .execute(self.db.write())
        .await?;
        Ok(())
    }

    pub async fn delete(&self, id: u64) -> Result<Vec<u64>> {
        let mut affected_ids = vec![];
        affected_ids.push(id);
        let mut tx = self.db.write().begin().await?;

        // first, clean up the temp id table
        let _ = sqlx::query!("DELETE FROM deleted_fs_entries")
            .execute(tx.as_mut())
            .await?;

        // then delete the inode
        let id = id as i64;
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
                affected_ids.push(r.id as u64);
            });

        // clean up the temp table again
        let _ = sqlx::query!("DELETE FROM deleted_fs_entries")
            .execute(tx.as_mut())
            .await?;

        tx.commit().await?;

        Ok(affected_ids)
    }

    pub async fn mv(
        &self,
        source_id: u64,
        target_dir: u64,
        target_name: String,
    ) -> Result<Vec<u64>> {
        let id = source_id as i64;
        let mut tx = self.db.write().begin().await?;
        let mut affected_ids = vec![];
        {
            let parent = target_dir as i64;
            sqlx::query!(
                "UPDATE fs_entries SET name = ?, parent = ? WHERE id = ?",
                target_name,
                parent,
                id
            )
            .execute(tx.as_mut())
            .await?;
        }

        // find all affected child-inodes
        sqlx::query!(
            "WITH RECURSIVE children AS (
                SELECT id, parent
                  FROM fs_entries
                  WHERE parent = ?

                UNION ALL

                SELECT e.id, e.parent
                  FROM fs_entries e
                  INNER JOIN children c ON e.parent = c.id
                  WHERE e.id != c.parent
            )
            SELECT DISTINCT(id) FROM children;",
            id
        )
        .fetch_all(tx.as_mut())
        .await?
        .into_iter()
        .for_each(|r| {
            affected_ids.push(r.id as u64);
        });

        tx.commit().await?;

        Ok(affected_ids)
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
    pub(super) fn new(id: u64, name: String, last_modified: DateTime<Utc>, parent: u64) -> Self {
        Self {
            id,
            name,
            last_modified,
            parent,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub(super) fn set_id(&mut self, id: u64) {
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
    pub(super) fn new(
        id: u64,
        name: String,
        size: u64,
        last_modified: DateTime<Utc>,
        parent: u64,
    ) -> Self {
        Self {
            id,
            name,
            size,
            last_modified,
            parent,
        }
    }

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

    pub(super) fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        &self.last_modified
    }

    pub(super) fn set_last_modified(&mut self, last_modified: DateTime<Utc>) {
        self.last_modified = last_modified;
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
    pub(super) fn new(
        id: u64,
        name: String,
        size: u64,
        last_modified: DateTime<Utc>,
        parent: u64,
        inode_type: InodeType,
    ) -> Self {
        match inode_type {
            InodeType::Directory => {
                Inode::Directory(Directory::new(id, name, last_modified, parent))
            }
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

    pub(super) fn set_id(&mut self, id: u64) {
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

    pub(super) fn to_path(&self, parent_path: &str) -> String {
        let tail = if self.inode_type() == InodeType::Directory {
            "/"
        } else {
            ""
        };
        format!("{}{}{}", parent_path, self.name(), tail)
    }
}

#[derive(FromRow)]
pub(super) struct InodeRecord {
    pub(super) id: i64,
    pub(super) name: String,
    pub(super) parent: i64,
    pub(super) entry_type: String,
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
