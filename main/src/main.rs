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
    #[arg(long, short = 'e', env)]
    /// URL for renterd's API endpoint (e.g., http://localhost:9880/api/).
    renterd_api_endpoint: Url,
    /// Password for the renterd API. It's recommended to use an environment variable for this.
    #[arg(long, short = 's', env)]
    renterd_api_password: String,
    /// Directory path to store persistent data. Will be created if it doesn't exist.
    #[arg(long, short = 'p', env)]
    data_path: PathBuf,
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

    tokio::fs::create_dir_all(&arguments.data_path).await?;

    let db_path = arguments.data_path.join("sia_nfs.sqlite");

    let sia_nfs = SiaNfs::new(
        &arguments.renterd_api_endpoint,
        &arguments.renterd_api_password,
        &db_path,
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
