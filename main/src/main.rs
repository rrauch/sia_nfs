use bytesize::ByteSize;
use clap::Parser;
use sia_nfs::SiaNfs;
use std::path::PathBuf;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{Instrument, Level};
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Parser)]
#[command(version)]
/// Exports Sia buckets via NFS.
/// Connects to renterd, allowing direct NFS access to exported buckets.
struct Arguments {
    #[arg(long, short = 'e', env, value_hint = clap::ValueHint::Url)]
    /// URL for renterd's API endpoint (e.g., http://localhost:9880/api/).
    renterd_api_endpoint: Url,
    /// Password for the renterd API. It's recommended to use an environment variable for this.
    #[arg(long, short = 's', env)]
    renterd_api_password: String,
    /// Directory to store persistent data in. Will be created if it doesn't exist.
    #[arg(long, short = 'd', env, value_hint = clap::ValueHint::DirPath)]
    data_dir: PathBuf,
    /// Optional directory to store the content cache in. Defaults to `DATA_DIR` if not set. Will be created if it doesn't exist.
    #[arg(long, short = 'c', env)]
    cache_dir: Option<PathBuf>,
    /// Maximum size of content cache. Set to `0` to disable.
    #[arg(long, short = 'm', env)]
    #[clap(default_value = "2 GiB")]
    max_cache_size: ByteSize,
    /// Host and port to listen on.
    #[arg(long, short = 'l', env)]
    #[clap(default_value = "localhost:12000")]
    listen_address: String,
    /// List of buckets to export.
    #[arg(required = true, num_args = 1..)]
    buckets: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        //.without_time()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let arguments = Arguments::parse();

    tokio::fs::create_dir_all(&arguments.data_dir).await?;
    let db_path = arguments.data_dir.join("sia_nfs.sqlite");

    let disk_cache = if arguments.max_cache_size.as_u64() > 0 {
        let cache_dir = arguments.cache_dir.unwrap_or_else(|| arguments.data_dir);
        tokio::fs::create_dir_all(&cache_dir).await?;
        let cache_db_path = cache_dir.join("sia_nfs_cache.sqlite");
        Some((cache_db_path, arguments.max_cache_size.as_u64()))
    } else {
        None
    };

    let sia_nfs = SiaNfs::new(
        &arguments.renterd_api_endpoint,
        &arguments.renterd_api_password,
        &db_path,
        disk_cache
            .as_ref()
            .map(|(path, size)| (path.as_path(), *size)),
        arguments.buckets,
        &arguments.listen_address,
    )
    .await?;

    let run_fut = sia_nfs.run();

    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();

    let span = tracing::trace_span!("main");

    async move {
        tokio::select! {
            _ = sigint.recv() => {
                tracing::info!("SIGINT received, shutting down")
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received, shutting down")
            }
            res = run_fut => {
                match res {
                    Ok(()) => tracing::info!("run finished, shutting down"),
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
        }
        Ok(())
    }
    .instrument(span)
    .await
}
