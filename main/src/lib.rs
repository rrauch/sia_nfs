mod io_scheduler;
mod nfs;
mod vfs;

use crate::nfs::SiaNfsFs;
use crate::vfs::Vfs;
use anyhow::Result;
use cachalot::Cachalot;
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use sqlx::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, Pool, Sqlite};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::log::LevelFilter;
use tracing::Instrument;
use url::Url;

pub struct SiaNfs {
    listener: NFSTcpListener<SiaNfsFs>,
}

impl SiaNfs {
    pub async fn new(
        renterd_endpoint: &Url,
        renterd_password: &str,
        db_path: &Path,
        disk_cache: Option<(&Path, u64)>,
        buckets: Vec<String>,
        listen_address: &str,
        uid: u32,
        gid: u32,
        file_mode: u32,
        dir_mode: u32,
    ) -> Result<Self> {
        let renterd = renterd_client::ClientBuilder::new()
            .api_endpoint_url(renterd_endpoint)
            .api_password(renterd_password)
            .verbose_logging(true)
            .build()?;

        let mut cachalot_builder = Cachalot::builder(&buckets);
        if let Some((cache_db_path, max_size)) = disk_cache {
            cachalot_builder = cachalot_builder
                .with_disk_cache(cache_db_path)
                .max_size(max_size)?
                .max_connections(20)?
                .build();
        } else {
            tracing::info!("Note: disk cache is currently DISABLED");
        };
        let cachalot = cachalot_builder.build().await?;

        let db = db_init(db_path, 20, true).await?;

        let vfs = Arc::new(
            Vfs::new(
                renterd,
                db,
                cachalot,
                &buckets,
                NonZeroUsize::new(5).unwrap(),
                NonZeroU64::new(1024 * 1024 * 1).unwrap(),
                Duration::from_secs(300),
            )
            .await?,
        );

        let mut listener = NFSTcpListener::bind(
            listen_address,
            SiaNfsFs::new(vfs, Duration::from_secs(10), uid, gid, file_mode, dir_mode),
        )
        .await?;
        listener.with_export_name("sia");

        Ok(Self { listener })
    }

    pub async fn run(self) -> Result<()> {
        self.listener.handle_forever().await?;
        Ok(())
    }
}

async fn db_init(
    db_file: &Path,
    max_connections: u8,
    create_if_missing: bool,
) -> Result<SqlitePool> {
    let writer = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with({
            SqliteConnectOptions::new()
                .create_if_missing(create_if_missing)
                .filename(db_file)
                .log_statements(LevelFilter::Trace)
                // `auto_vacuum` needs to be executed before `journal_mode`
                .auto_vacuum(SqliteAutoVacuum::Full)
                .journal_mode(SqliteJournalMode::Wal)
                .foreign_keys(true)
                .pragma("recursive_triggers", "ON")
                .busy_timeout(Duration::from_millis(100))
                .shared_cache(true)
        })
        .await?;

    async { sqlx::migrate!("./migrations").run(&writer).await }
        .instrument(tracing::warn_span!("db_migration"))
        .await?;

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
