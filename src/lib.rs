mod nfs;
mod vfs;

use crate::nfs::SiaNfsFs;
use crate::vfs::{Inode, Vfs};
use anyhow::Result;
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use sqlx::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, Pool, Sqlite};
use std::path::Path;
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
        buckets: Vec<String>,
    ) -> Result<Self> {
        let renterd = renterd_client::ClientBuilder::new()
            .api_endpoint_url(renterd_endpoint)
            .api_password(renterd_password)
            .verbose_logging(true)
            .build()?;

        let db = db_init(db_path, 20, true).await?;

        let vfs = Vfs::new(renterd, db, buckets).await?;
        //Self::foo(&vfs).await?;

        Ok(Self {
            listener: NFSTcpListener::bind("127.0.0.1:12000", SiaNfsFs::new(vfs)).await?,
        })
    }

    pub async fn run(self) -> Result<()> {
        self.listener.handle_forever().await?;
        Ok(())
    }

    /*pub async fn foo(vfs: &Vfs) -> Result<()> {
        //let x = self.vfs.inode_by_name_parent("Montserrat.zip", 10006).await?;
        //let x = self.vfs.inode_by_id(100010).await?;
        let x = vfs.root().clone();
        for inode in vfs.read_dir(&x).await? {
            println!("{:?}", inode);
            if let Inode::Directory(dir) = inode {
                for inode in vfs.read_dir(&dir).await? {
                    println!(" {:?}", inode);
                    if let Inode::Directory(dir) = inode {
                        for inode in vfs.read_dir(&dir).await? {
                            println!("  {:?}", inode);
                            if let Inode::Directory(dir) = inode {
                                for inode in vfs.read_dir(&dir).await? {
                                    println!("   {:?}", inode);
                                    if let Inode::Directory(dir) = inode {
                                        for inode in vfs.read_dir(&dir).await? {
                                            println!("    {:?}", inode);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        todo!()
    }
     */
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
