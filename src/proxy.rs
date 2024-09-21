use std::sync::Arc;
use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};
use pgwire::api::query::{SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::tokio::process_socket;

use crate::config::ProxyConfig;
use crate::auth::AuthHandler;

pub struct TwentyProxyProcessor {
    upstream_client: Client,
    config: Arc<ProxyConfig>,
}

impl TwentyProxyProcessor {
    pub fn new(upstream_client: Client, config: Arc<ProxyConfig>) -> Self {
        Self {
            upstream_client,
            config,
        }
    }

    async fn authorize_query(&self, client: &impl ClientInfo, query: &str) -> PgWireResult<()> {
        // Implement query authorization logic 
        // example,  if the query is targeting the allowed schema
        if !query.to_lowercase().contains(&format!("schema {}", client.get_database())) {
            return Err(PgWireError::UserError(Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Query not authorized for this schema",
            ))));
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for TwentyProxyProcessor {
    async fn do_query<'a, C>(&self, client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.authorize_query(client, query).await?;

        let rows = self.upstream_client
            .simple_query(query)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let mut downstream_response = Vec::new();
        let mut row_buf = Vec::new();
        let mut field_info: Vec<FieldInfo> = Vec::new();
        
        for row in rows {
            match row {
                tokio_postgres::SimpleQueryMessage::CommandComplete(tag) => {
                    if !row_buf.is_empty() {
                        let query_response = QueryResponse::new(field_info.clone(), row_buf.clone());
                        downstream_response.push(Response::Query(query_response));
                        row_buf.clear();
                    }
                    downstream_response.push(Response::Execution(
                        Tag::new_for_execution(&tag, None),
                    ));
                }
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    if field_info.is_empty() {
                        for column in row.columns() {
                            field_info.push(FieldInfo::new(
                                column.name().to_string(),
                                None,
                                Type::VARCHAR, 
                                Default::default(),
                            ));
                        }
                    }
                    
                    let mut encoder = DataRowEncoder::new(field_info.len());
                    for i in 0..row.len() {
                        if let Some(val) = row.get(i) {
                            encoder.encode_text(val)?;
                        } else {
                            encoder.encode_null()?;
                        }
                    }
                    row_buf.push(encoder.finish());
                }
                _ => {}
            }
        }

        Ok(downstream_response)
    }
}

pub async fn run_proxy(config: ProxyConfig) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(config);
    let (client, connection) = tokio_postgres::connect(&config.upstream_connection_string, NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Upstream connection error: {}", e);
        }
    });

    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(TwentyProxyProcessor::new(
        client,
        config.clone(),
    ))));

    let auth_handler = Arc::new(StatelessMakeHandler::new(Arc::new(AuthHandler::new(config.clone()))));

    let listener = TcpListener::bind(&config.listen_address).await?;
    println!("Listening on {}", config.listen_address);

    loop {
        let (socket, _) = listener.accept().await?;
        let processor_ref = processor.make();
        let auth_handler_ref = auth_handler.make();
        
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, auth_handler_ref, processor_ref).await {
                eprintln!("Error processing connection: {}", e);
            }
        });
    }
}
