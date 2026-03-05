use anyhow::Result;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

pub fn init_logger() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();
    tracing::subscriber::set_global_default(subscriber).map_err(|e| anyhow::anyhow!("logger init failed: {}", e))
}
