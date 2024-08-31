use anyhow::{anyhow, bail};
use bytes::Bytes;
use sqlx::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, Pool, Sqlite};
use std::num::NonZeroU64;
use std::path::Path;
use std::time::Duration;
use tracing::log::LevelFilter;
use tracing::Instrument;

const VALID_PAGE_SIZES: [u32; 8] = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];

pub struct Cachalot {
    db: SqlitePool,
    page_size: u32,
    max_pages: u64,
}

impl Cachalot {
    pub async fn new(
        db_file: &Path,
        page_size: u32,
        max_pages: NonZeroU64,
        max_db_connections: u8,
        buckets: &Vec<String>,
    ) -> anyhow::Result<Self> {
        let db = db_init(db_file, page_size, max_db_connections, true, buckets).await?;
        Ok(Self {
            db,
            page_size,
            max_pages: max_pages.get(),
        })
    }

    pub async fn get(
        &self,
        bucket: &str,
        path: &str,
        version: Bytes,
        page: u64,
    ) -> anyhow::Result<Option<Bytes>> {
        let version = version.as_ref();
        let page = page as i64;
        Ok(sqlx::query!(
            "
        SELECT p.content
        FROM pages p
        JOIN files f ON p.file_id = f.id
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
        version: Bytes,
        page: u64,
        content: Bytes,
    ) -> anyhow::Result<()> {
        let version = version.as_ref();
        let page = page as i64;
        let content = content.as_ref();

        let mut tx = self.db.write().begin().await?;
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
            "INSERT INTO pages (file_id, page_nr, content)
            VALUES (
                (SELECT id FROM files WHERE bucket = ? AND path = ? AND version = ?),
                ?, ?
            )
            ON CONFLICT (file_id, page_nr) DO UPDATE SET content = excluded.content;",
            bucket,
            path,
            version,
            page,
            content
        )
        .execute(tx.as_mut())
        .await?;

        tx.commit().await?;
        Ok(())
    }
}

async fn db_init(
    db_file: &Path,
    page_size: u32,
    max_connections: u8,
    create_if_missing: bool,
    buckets: &Vec<String>,
) -> anyhow::Result<SqlitePool> {
    if !VALID_PAGE_SIZES.contains(&page_size) {
        bail!("invalid page size: {}", page_size);
    }

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

    if current_page_size != page_size {
        bail!(
            "database file has incompatible page size, {} != {}",
            current_page_size,
            page_size
        );
    }

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
