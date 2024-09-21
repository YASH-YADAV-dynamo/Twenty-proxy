mod config;
mod proxy;
mod auth;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = config::ProxyConfig::from_env()?;
    proxy::run_proxy(config).await
}