use sia_nfs::SiaNfs;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::Level;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        //.without_time()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::DEBUG.into())
                .from_env_lossy(),
        )
        .init();

    let args = env::args().into_iter().collect::<Vec<_>>();
    let renterd_endpoint = args.get(1).expect("renterd endpoint not set").as_str();
    let renterd_password = args.get(2).expect("renterd password not set").as_str();
    let db_path = args.get(3).expect("db_path not set").as_str();

    let sia_nfs = SiaNfs::new(
        &renterd_endpoint.try_into()?,
        renterd_password,
        PathBuf::from_str(db_path)?.as_path(),
        vec!["default".to_string()],
    )
    .await?;

    sia_nfs.run().await?;

    println!("");
    Ok(())
}
