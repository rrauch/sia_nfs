mod housekeeper;

use crate::disk_cache::housekeeper::Housekeeper;
use crate::{content_hash, CachalotBuilder, SqlitePool};
use anyhow::{anyhow, bail};
use bytes::Bytes;
use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, Sqlite, Transaction};
use std::cmp::{max, min};
use std::num::{NonZeroU64, NonZeroU8};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::fs::remove_file;
use tokio::task::JoinHandle;
use tracing::log::LevelFilter;
use tracing::Instrument;

pub struct DiskCacheBuilder {
    path: PathBuf,
    max_size: u64,
    max_connections: u8,
    ttl: Duration,
    cachalot_builder: CachalotBuilder,
}

impl DiskCacheBuilder {
    pub(super) fn new<P: AsRef<Path>>(
        path: P,
        max_size: u64,
        max_connections: u8,
        cachalot_builder: CachalotBuilder,
    ) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            max_size,
            max_connections,
            ttl: Duration::from_secs(86400 * 7),
            cachalot_builder,
        }
    }

    pub fn max_size<T: TryInto<NonZeroU64>>(mut self, max_size: T) -> anyhow::Result<Self> {
        self.max_size = max_size
            .try_into()
            .map_err(|_| anyhow!("max_size invalid"))?
            .get();
        Ok(self)
    }

    pub fn max_connections<T: TryInto<NonZeroU8>>(
        mut self,
        max_connections: T,
    ) -> anyhow::Result<Self> {
        self.max_connections = max_connections
            .try_into()
            .map_err(|_| anyhow!("max_connections invalid"))?
            .get();
        Ok(self)
    }

    pub fn time_to_live(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn build(self) -> CachalotBuilder {
        let mut cb = self.cachalot_builder;
        cb.disk_cache = Some((self.path, self.max_size, self.ttl, self.max_connections));
        cb
    }
}

#[derive(Clone)]
struct Config {
    db: SqlitePool,
    path: PathBuf,
    page_size: u32,
    min_free_pages: u32,
    ttl: Duration,
    usable_pages_est: Arc<AtomicU32>,
}

impl Config {
    fn new(db: SqlitePool, path: &Path, page_size: u32, max_pages: u32, ttl: Duration) -> Self {
        // 10% of max size, but between 1MiB and 1GiB
        let one_mib_pages = (1024u32 * 1024).div_ceil(page_size);
        let one_gib_pages = (1024u32 * 1024 * 1024).div_ceil(page_size);
        let ten_percent_pages = max_pages.div_ceil(10);
        let min_free_pages = min(max(ten_percent_pages, one_mib_pages), one_gib_pages);

        Self {
            db,
            path: path.to_path_buf(),
            page_size,
            min_free_pages,
            ttl,
            usable_pages_est: Arc::new(AtomicU32::new(0)),
        }
    }
}

pub(crate) struct DiskCache {
    config: Config,
    _housekeeping: JoinHandle<()>,
}

impl Drop for DiskCache {
    fn drop(&mut self) {
        self._housekeeping.abort();
    }
}

impl DiskCache {
    pub(super) async fn new(
        db_file: &Path,
        page_size: u32,
        max_size: u64,
        max_db_connections: u8,
        ttl: Duration,
        buckets: Vec<String>,
    ) -> anyhow::Result<Self> {
        let max_pages = u32::try_from(max_size.div_ceil(page_size as u64)).unwrap_or(u32::MAX);

        let db = db_init(db_file, page_size, max_pages, max_db_connections, true).await?;

        let config = Config::new(db, db_file, page_size, max_pages, ttl);

        let mut tx = config.db.write().begin().await?;

        let db_buckets = sqlx::query!("SELECT DISTINCT bucket FROM files")
            .fetch_all(tx.as_mut())
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
            for bucket in obsolete_buckets {
                sqlx::query!("DELETE FROM files WHERE bucket = ?", bucket)
                    .execute(tx.as_mut())
                    .await?;
            }
        }

        remove_expired(&mut tx, &config).await?;

        free_pages(&mut tx, &config).await?;

        let usable_pages = usable_pages(&mut tx, &config).await?;

        tx.commit().await?;

        sqlx::query!("VACUUM").execute(config.db.write()).await?;

        config
            .usable_pages_est
            .store(usable_pages, Ordering::SeqCst);

        let _housekeeping = {
            let mut housekeeper = Housekeeper::new(config.clone());
            tokio::spawn(async move {
                housekeeper.run().await;
                tracing::info!("housekeeper stopped");
            })
        };

        Ok(Self {
            config,
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
        .fetch_optional(self.config.db.read())
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
        let content_len = content.len() as u32;
        let content_hash = content_hash(content);
        let content_hash = content_hash.as_bytes().as_slice();

        let mut tx = self.config.db.write().begin().await?;
        let page_count_start = page_count(&mut tx).await?;

        let mut freed_pages = 0;
        // delete any other version of this file first
        if sqlx::query!(
            "DELETE FROM files WHERE bucket = ? AND path = ? and version != ?",
            bucket,
            path,
            version
        )
        .execute(tx.as_mut())
        .await?
        .rows_affected()
            > 0
        {
            freed_pages = page_count(&mut tx).await? - page_count_start;
        }

        let pages_needed = content_len.div_ceil(self.config.page_size) + 1;

        // this is a quick estimate
        let extra_pages_needed = {
            let usable_pages = self.config.usable_pages_est.load(Ordering::SeqCst) + freed_pages;
            if usable_pages >= pages_needed {
                0
            } else {
                pages_needed - usable_pages
            }
        };

        if extra_pages_needed > 0 {
            let pages_freed = try_free_pages(extra_pages_needed, &mut tx, &self.config).await?;
            if pages_freed < extra_pages_needed {
                tracing::warn!(
                    "unable to free enough pages, required {} but only {} were freed",
                    extra_pages_needed,
                    pages_freed
                );
            }
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

        let page_count_end = page_count(&mut tx).await?;
        tx.commit().await?;

        let page_diff = page_count_end as i64 - page_count_start as i64;
        if page_diff != 0 {
            // update usable_pages estimation
            if page_diff > 0 {
                self.config
                    .usable_pages_est
                    .fetch_add(page_diff as u32, Ordering::SeqCst);
            } else {
                let page_diff = page_diff * -1;
                self.config
                    .usable_pages_est
                    .fetch_sub((page_diff * -1) as u32, Ordering::SeqCst);
            }
        }

        tracing::trace!(page_count_change = page_diff, "put");

        Ok(())
    }
}

async fn try_free_pages(
    pages_to_free: u32,
    tx: &mut Transaction<'_, Sqlite>,
    config: &Config,
) -> anyhow::Result<u32> {
    let mut freed_pages = 0;
    let mut current_page_count = page_count(tx).await?;

    while freed_pages < pages_to_free {
        let expiration_deadline = Utc::now() - config.ttl;
        let page_count_before = current_page_count;
        let rows_affected = sqlx::query!(
            "
        DELETE FROM content
        WHERE content_hash = (
            SELECT content_hash
            FROM content
            ORDER BY
                (created < ?) DESC,
                created ASC,
                last_referenced ASC,
                num_pages ASC
            LIMIT 1
        )",
            expiration_deadline
        )
        .execute(tx.as_mut())
        .await?
        .rows_affected();

        if rows_affected == 0 {
            // nothing left to delete
            break;
        }
        current_page_count = page_count(tx).await?;
        freed_pages += page_count_before - current_page_count;
    }

    tracing::debug!(pages_freed = freed_pages, "freed pages");

    Ok(freed_pages)
}

async fn page_count(tx: &mut Transaction<'_, Sqlite>) -> anyhow::Result<u32> {
    sqlx::query!("PRAGMA page_count")
        .fetch_one(tx.as_mut())
        .await?
        .page_count
        .map(|c| c as u32)
        .ok_or(anyhow!("unable to get page_count from database"))
}

async fn max_page_count(tx: &mut Transaction<'_, Sqlite>) -> anyhow::Result<u32> {
    sqlx::query!("PRAGMA max_page_count")
        .fetch_one(tx.as_mut())
        .await?
        .max_page_count
        .map(|c| c as u32)
        .ok_or(anyhow!("unable to get max_page_count from database"))
}

async fn freelist_count(tx: &mut Transaction<'_, Sqlite>) -> anyhow::Result<u32> {
    sqlx::query!("PRAGMA freelist_count")
        .fetch_one(tx.as_mut())
        .await?
        .freelist_count
        .map(|c| c as u32)
        .ok_or(anyhow!("unable to get freelist_count from database"))
}
async fn usable_pages(tx: &mut Transaction<'_, Sqlite>, config: &Config) -> anyhow::Result<u32> {
    let max_pages_count = max_page_count(tx).await?;
    let page_count = page_count(tx).await?;

    let available_pages = max_pages_count.saturating_sub(page_count);
    if available_pages == 0 {
        return Ok(0);
    }

    let freelist_count = freelist_count(tx).await?;

    let path = config.path.clone();
    let available_disk_space =
        tokio::task::spawn_blocking(move || fs4::available_space(&path)).await??;

    let potential_pages =
        u32::try_from(available_disk_space / config.page_size as u64).unwrap_or(u32::MAX);

    let usable = min(potential_pages + freelist_count, available_pages);
    Ok(usable)
}

async fn remove_expired(tx: &mut Transaction<'_, Sqlite>, config: &Config) -> anyhow::Result<bool> {
    let deadline = Utc::now() - config.ttl;

    let did_work = sqlx::query!("DELETE FROM content WHERE created <= ?", deadline)
        .execute(tx.as_mut())
        .await?
        .rows_affected()
        > 0;

    Ok(did_work)
}
async fn free_pages(tx: &mut Transaction<'_, Sqlite>, config: &Config) -> anyhow::Result<()> {
    let extra_pages_needed = {
        let usable_pages = usable_pages(tx, config).await?;
        if usable_pages >= config.min_free_pages {
            0
        } else {
            config.min_free_pages - usable_pages
        }
    };

    if extra_pages_needed > 0 {
        tracing::info!(
            extra_pages_needed = extra_pages_needed,
            "freeing pages from disk cache"
        );
        let pages_freed = try_free_pages(extra_pages_needed, tx, config).await?;
        tracing::info!(pages_freed = pages_freed, "pages freed");
    }

    Ok(())
}

async fn db_init(
    db_file: &Path,
    page_size: u32,
    max_pages: u32,
    max_connections: u8,
    create_if_missing: bool,
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
                    .pragma("max_page_count", format!("{}", max_pages))
                    .filename(db_file)
                    .log_statements(LevelFilter::Trace)
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(Duration::from_millis(100))
                    .shared_cache(true)
            })
            .await?;

        async { sqlx::migrate!("./migrations").run(&writer).await }
            .instrument(tracing::warn_span!("db_migration"))
            .await?;

        // check the page size

        let current_page_size = sqlx::query!("PRAGMA page_size")
            .fetch_one(&writer)
            .await?
            .page_size
            .map(|c| c as u32)
            .ok_or(anyhow!("unable to get page_size from database"))?;

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
