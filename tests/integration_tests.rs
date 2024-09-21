use tokio_postgres::{NoTls, Error};
use twenty_postgres_proxy::config::ProxyConfig;
use twenty_postgres_proxy::proxy::run_proxy;

#[tokio::test]
async fn test_proxy_connection() -> Result<(), Error> {
    let config = ProxyConfig {
        listen_address: "127.0.0.1:5432".to_string(),
        upstream_connection_string: "host=localhost user=testuser dbname=testdb".to_string(),
        allowed_schemas: vec!["public".to_string()],
    };

    tokio::spawn(async move {
        if let Err(e) = run_proxy(config).await {
            eprintln!("Proxy error: {}", e);
        }
    });

    // Give the proxy some time to start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Try to connect through the proxy
    let (client, connection) =
        tokio_postgres::connect("host=127.0.0.1 user=testuser dbname=public", NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Test a simple query
    let rows = client
        .query("SELECT 1", &[])
        .await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);

    Ok(())
}