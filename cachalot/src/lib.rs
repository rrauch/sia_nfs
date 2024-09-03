use anyhow::{anyhow, bail};
use blake3::Hash;
use bytes::Bytes;
use futures_util::stream::TryStreamExt;
use moka::future::{Cache, CacheBuilder};
use sqlx::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, Pool, Sqlite, Transaction};
use std::cmp::{max, min};
use std::future::Future;
use std::num::{NonZeroU64, NonZeroU8};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::fs::remove_file;
use tokio::task::JoinHandle;
use tracing::log::LevelFilter;
use tracing::Instrument;

const VALID_PAGE_SIZES: [u32; 8] = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];
const CONTENT_HASH_SEED: [u8; 32] = [
    0xf9, 0xa2, 0x9a, 0xe8, 0xe1, 0xe3, 0x26, 0x91, 0x57, 0xab, 0x79, 0x15, 0x92, 0xc9, 0x6f, 0x2e,
    0x92, 0xef, 0xfd, 0x66, 0x59, 0x85, 0xc0, 0xd3, 0x32, 0xc7, 0x13, 0x35, 0xb4, 0x71, 0x29, 0x14,
];

pub struct Cachalot {
    mem_cache: Cache<(String, String, u64, u64), Bytes>,
    disk_cache: Option<DiskCache>,
    page_size: usize,
}

pub struct PageSize(u32);
impl TryFrom<u32> for PageSize {
    type Error = u32;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if !VALID_PAGE_SIZES.contains(&value) {
            return Err(value);
        }
        Ok(PageSize(value))
    }
}

pub struct CachalotBuilder {
    buckets: Vec<String>,
    page_size: u32,
    max_mem_cache: u64,
    disk_cache: Option<(PathBuf, u64, u8)>,
}

impl CachalotBuilder {
    pub fn with_disk_cache<P: AsRef<Path>, S: TryInto<NonZeroU64>, C: TryInto<NonZeroU8>>(
        mut self,
        db_file: P,
        max_size: S,
        max_connections: C,
    ) -> anyhow::Result<Self> {
        self.disk_cache = Some((
            db_file.as_ref().to_path_buf(),
            max_size
                .try_into()
                .map_err(|_| anyhow!("max_size cannot be zero"))?
                .get(),
            max_connections
                .try_into()
                .map_err(|_| anyhow!("max_connections cannot be zero"))?
                .get(),
        ));
        Ok(self)
    }

    pub fn page_size<P: TryInto<PageSize>>(mut self, page_size: P) -> anyhow::Result<Self> {
        self.page_size = page_size
            .try_into()
            .map_err(|_| anyhow!("page_size invalid"))?
            .0;
        Ok(self)
    }

    pub fn max_mem_cache<M: TryInto<NonZeroU64>>(
        mut self,
        max_mem_cache: M,
    ) -> anyhow::Result<Self> {
        self.max_mem_cache = max_mem_cache
            .try_into()
            .map_err(|_| anyhow!("max_mem_cache cannot be zero"))?
            .get();
        Ok(self)
    }

    pub async fn build(self) -> anyhow::Result<Cachalot> {
        if self.max_mem_cache < self.page_size as u64 {
            bail!("max_mem_cache cannot be smaller than page size");
        }

        let mem_cache = CacheBuilder::new(self.max_mem_cache)
            .weigher(|_, v: &Bytes| v.len() as u32)
            .build();

        let disk_cache = if let Some((path, max_size, max_connections)) = self.disk_cache {
            if max_size < self.page_size as u64 {
                bail!("max_disk_cache cannot be smaller than page size");
            }
            let max_pages = max_size / self.page_size as u64;
            Some(
                DiskCache::new(
                    path.as_path(),
                    self.page_size,
                    max_pages,
                    max_connections,
                    self.buckets,
                )
                .await?,
            )
        } else {
            None
        };

        Ok(Cachalot {
            mem_cache,
            disk_cache,
            page_size: self.page_size as usize,
        })
    }
}

impl Cachalot {
    pub fn builder<I, T>(buckets: I) -> CachalotBuilder
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        CachalotBuilder {
            buckets: buckets
                .into_iter()
                .map(|s| s.as_ref().to_string())
                .collect(),
            page_size: PageSize::try_from(32768).unwrap().0,
            max_mem_cache: 1024 * 1024 * 100,
            disk_cache: None,
        }
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub async fn try_get_with<F>(
        &self,
        bucket: &str,
        path: &str,
        version: u64,
        page: u64,
        init: F,
    ) -> anyhow::Result<Bytes>
    where
        F: Future<Output = anyhow::Result<Bytes>>,
    {
        Ok(self
            .mem_cache
            .try_get_with::<_, anyhow::Error>(
                (bucket.to_string(), path.to_string(), version, page),
                async {
                    if let Some(disk_cache) = self.disk_cache.as_ref() {
                        if let Some(content) = disk_cache.get(bucket, path, version, page).await? {
                            return Ok(content);
                        }
                    }

                    let content = init.await?;

                    if let Some(disk_cache) = self.disk_cache.as_ref() {
                        disk_cache
                            .put(bucket, path, version, page, content.clone())
                            .await?;
                    }

                    Ok(content)
                },
            )
            .await
            .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!(e))?)
    }
}

struct DiskCache {
    db: SqlitePool,
    max_size: u64,
    page_size: u32,
    _housekeeping: JoinHandle<()>,
}

impl Drop for DiskCache {
    fn drop(&mut self) {
        self._housekeeping.abort();
    }
}

impl DiskCache {
    async fn new(
        db_file: &Path,
        page_size: u32,
        max_pages: u64,
        max_db_connections: u8,
        buckets: Vec<String>,
    ) -> anyhow::Result<Self> {
        let db = db_init(db_file, page_size, max_db_connections, true, &buckets).await?;

        let max_size = max_pages * page_size as u64;
        // 10% of max size, but between 1MiB and 1GiB
        let min_free = min(max(max_size / 10, 1024 * 1024), 1024 * 1024 * 1024);

        let _housekeeping = {
            let db = db.clone();
            tokio::spawn(async move { housekeeping(db, max_size as i64, min_free as i64).await })
        };

        Ok(Self {
            db,
            max_size,
            page_size,
            _housekeeping,
        })
    }

    pub async fn get(
        &self,
        bucket: &str,
        path: &str,
        version: u64,
        page: u64,
    ) -> anyhow::Result<Option<Bytes>> {
        let page = page as i64;
        let version = version as i64;
        Ok(sqlx::query!(
            "
        SELECT c.content
        FROM files f
        JOIN pages p ON f.id = p.file_id
        JOIN content c ON p.content_hash = c.content_hash
        WHERE f.bucket = ? AND f.path = ? AND f.version = ? AND p.page_nr = ?",
            bucket,
            path,
            version,
            page,
        )
        .fetch_optional(self.db.read())
        .await?
        .map(|r| Bytes::from(r.content)))
    }

    pub async fn put(
        &self,
        bucket: &str,
        path: &str,
        version: u64,
        page: u64,
        content: Bytes,
    ) -> anyhow::Result<()> {
        let page = page as i64;
        let version = version as i64;
        let content = content.as_ref();
        let content_hash = content_hash(content);
        let content_hash = content_hash.as_bytes().as_slice();

        let mut tx = self.db.write().begin().await?;
        // delete any other version of this file first
        sqlx::query!(
            "DELETE FROM files WHERE bucket = ? AND path = ? and version != ?",
            bucket,
            path,
            version
        )
        .execute(tx.as_mut())
        .await?;

        let extra_space_needed = {
            let free_after =
                self.max_size as i64 - used_space(&mut tx).await? as i64 - self.page_size as i64;
            if free_after < 0 {
                free_after * -1
            } else {
                0
            }
        } as u64;

        if extra_space_needed > 0 {
            let _ = try_free_space(extra_space_needed, &mut tx).await?;
        }

        sqlx::query!(
            "
            INSERT INTO files (bucket, path, version)
            VALUES (?, ?, ?)
                ON CONFLICT (bucket, path, version) DO NOTHING;",
            bucket,
            path,
            version
        )
        .execute(tx.as_mut())
        .await?;

        sqlx::query!(
            "
            INSERT INTO content (content_hash, content)
            VALUES (?, ?)
                ON CONFLICT(content_hash) DO NOTHING;
            ",
            content_hash,
            content
        )
        .execute(tx.as_mut())
        .await?;

        sqlx::query!(
            "
            INSERT INTO pages (file_id, page_nr, content_hash)
            VALUES (
                (SELECT id FROM files WHERE bucket = ? AND path = ? AND version = ?),
                ?, ?
            )
            ON CONFLICT (file_id, page_nr) DO UPDATE SET content_hash = excluded.content_hash;",
            bucket,
            path,
            version,
            page,
            content_hash,
        )
        .execute(tx.as_mut())
        .await?;

        tx.commit().await?;
        Ok(())
    }
}

async fn try_free_space(
    min_bytes_to_free: u64,
    tx: &mut Transaction<'_, Sqlite>,
) -> anyhow::Result<u64> {
    let used_before = used_space(tx).await?;

    let mut bytes_needed = min_bytes_to_free;
    let mut content_hashes = vec![];

    let mut stream = sqlx::query!(
        "
            SELECT content_hash, content_length
            FROM content
            ORDER BY last_referenced ASC, num_pages ASC"
    )
    .fetch(tx.as_mut());

    while let Some(res) = stream.try_next().await? {
        content_hashes.push(res.content_hash);
        bytes_needed = bytes_needed.saturating_sub(res.content_length as u64);
        if bytes_needed == 0 {
            break;
        }
    }
    drop(stream);

    if bytes_needed != 0 {
        bail!(
            "unable to free enough space; '{}' missing to reach minimum required '{}'",
            bytes_needed,
            min_bytes_to_free
        );
    }

    for content_hash in content_hashes {
        sqlx::query!("DELETE FROM content WHERE content_hash = ?", content_hash)
            .execute(tx.as_mut())
            .await?;
    }

    let used_after = used_space(tx).await?;
    let actual_freed = used_before - used_after;
    if actual_freed < min_bytes_to_free {
        bail!(
            "unable to free enough space; minimum required '{}' <> '{}' actual",
            min_bytes_to_free,
            actual_freed
        );
    }

    tracing::debug!("freed {} bytes from disk cache", actual_freed);

    Ok(actual_freed)
}

async fn used_space(tx: &mut Transaction<'_, Sqlite>) -> anyhow::Result<u64> {
    Ok(sqlx::query!("SELECT total_size FROM content_stats")
        .fetch_one(tx.as_mut())
        .await?
        .total_size as u64)
}

async fn housekeeping(db: SqlitePool, max_size: i64, min_free: i64) {
    let mut next_run_in = Duration::from_secs(300);
    let mut next_vacuum = SystemTime::now() + Duration::from_secs(3600);
    loop {
        tokio::time::sleep(next_run_in).await;
        next_run_in = Duration::from_secs(300);
        if let Err(err) = hk_free_space(&db, max_size, min_free).await {
            tracing::error!(error = %err, "error trying to free space in disk cache");
            next_run_in = Duration::from_secs(900);
        } else {
            next_vacuum = SystemTime::now();
        }
        if next_vacuum <= SystemTime::now() {
            let _ = sqlx::query!("VACUUM").execute(db.write()).await;
            next_vacuum = SystemTime::now() + Duration::from_secs(3600);
        }
    }
}

async fn hk_free_space(db: &SqlitePool, max_size: i64, min_free: i64) -> anyhow::Result<()> {
    let mut tx = db.writer.begin().await?;

    let extra_space_needed = {
        let free = max_size - used_space(&mut tx).await? as i64;
        if free <= min_free {
            min_free - free
        } else {
            0
        }
    } as u64;

    if extra_space_needed > 0 {
        tracing::info!("freeing extra {} bytes from disk cache", extra_space_needed);
        let space_freed = try_free_space(extra_space_needed, &mut tx).await?;
        tracing::info!("{} bytes freed", space_freed);
    }
    tx.commit().await?;
    Ok(())
}

async fn db_init(
    db_file: &Path,
    page_size: u32,
    max_connections: u8,
    create_if_missing: bool,
    buckets: &Vec<String>,
) -> anyhow::Result<SqlitePool> {
    let mut attempt = 0;
    let writer = loop {
        attempt += 1;
        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with({
                SqliteConnectOptions::new()
                    .create_if_missing(create_if_missing)
                    .page_size(page_size)
                    .filename(db_file)
                    .log_statements(LevelFilter::Trace)
                    // `auto_vacuum` needs to be executed before `journal_mode`
                    .auto_vacuum(SqliteAutoVacuum::Full)
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(Duration::from_millis(100))
                    .shared_cache(true)
            })
            .await?;

        async { sqlx::migrate!("./migrations").run(&writer).await }
            .instrument(tracing::warn_span!("db_migration"))
            .await?;

        // check the page size
        let current_page_size = current_page_size(&writer).await?;
        if page_size == current_page_size {
            // all good
            break writer;
        }

        writer.close().await;
        drop(writer);

        if attempt > 1 {
            // we already tried to recreate the database once
            // something is wrong here, cannot continue
            bail!(
                "unable to create the database at path {} with page_size {}",
                db_file.display(),
                page_size
            );
        }
        // delete the database file and recreate it
        tracing::info!("database at {} has wrong page_size ({} <> {}), it will be deleted and recreated with the correct page size", db_file.display(), current_page_size, page_size);

        let wal_path = db_file
            .with_extension("")
            .with_file_name(format!("{}-wal", db_file.display()));
        let shm_path = db_file
            .with_extension("")
            .with_file_name(format!("{}-shm", db_file.display()));

        tracing::info!("deleting {}", db_file.display());
        remove_file(db_file).await?;

        if fs::metadata(&wal_path).await.is_ok() {
            tracing::info!("deleting {}", wal_path.display());
            remove_file(&wal_path).await?;
        }

        if fs::metadata(&shm_path).await.is_ok() {
            tracing::info!("deleting {}", shm_path.display());
            remove_file(&shm_path).await?;
        }

        continue;
    };

    let db_buckets = sqlx::query!("SELECT DISTINCT bucket FROM files")
        .fetch_all(&writer)
        .await?
        .into_iter()
        .map(|r| r.bucket)
        .collect::<Vec<_>>();

    let obsolete_buckets = db_buckets
        .into_iter()
        .filter(|b| !buckets.contains(b))
        .collect::<Vec<_>>();

    if !obsolete_buckets.is_empty() {
        tracing::info!("removing data for obsolete buckets");
        let mut tx = writer.begin().await?;
        for bucket in obsolete_buckets {
            sqlx::query!("DELETE FROM files WHERE bucket = ?", bucket)
                .execute(tx.as_mut())
                .await?;
        }
        tx.commit().await?;
    }

    sqlx::query!("VACUUM").execute(&writer).await?;

    let reader = SqlitePoolOptions::new()
        .max_connections(max_connections as u32)
        .connect_with({
            SqliteConnectOptions::new()
                .create_if_missing(false)
                .filename(db_file)
                .log_statements(LevelFilter::Trace)
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(Duration::from_millis(100))
                .shared_cache(true)
                .pragma("query_only", "ON")
        })
        .await?;

    Ok(SqlitePool { writer, reader })
}

async fn current_page_size(db: &Pool<Sqlite>) -> anyhow::Result<u32> {
    Ok(sqlx::query!("PRAGMA page_size")
        .fetch_one(db)
        .await?
        .page_size
        .map(|p| p as u32)
        .ok_or(anyhow!("cannot get page_size from database"))?)
}

fn content_hash(content: &[u8]) -> Hash {
    let mut hasher = blake3::Hasher::new_keyed(&CONTENT_HASH_SEED);
    hasher.update("cachalot content hash v1 start\n".as_bytes());
    hasher.update("length:".as_bytes());
    hasher.update(content.len().to_le_bytes().as_slice());
    hasher.update("\ncontent:".as_bytes());
    hasher.update(content);
    hasher.update("\ncachalot content hash v1 end".as_bytes());
    hasher.finalize()
}

#[derive(Debug, Clone)]
pub(crate) struct SqlitePool {
    writer: Pool<Sqlite>,
    reader: Pool<Sqlite>,
}

impl SqlitePool {
    pub fn read(&self) -> &Pool<Sqlite> {
        &self.reader
    }

    pub fn write(&self) -> &Pool<Sqlite> {
        &self.writer
    }
}
