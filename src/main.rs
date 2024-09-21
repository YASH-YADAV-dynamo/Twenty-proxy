use std::sync::Arc;
use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};
use pgwire::api::query::{SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::tokio::process_socket;

// Struct and Implementation Definitions
pub struct ProxyProcessor {
    upstream_client: Client,
}

#[async_trait]
impl SimpleQueryHandler for ProxyProcessor {
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
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
                        // Initialize field info based on row
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

// Main Function
#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect("host=127.0.0.1 user=postgres", NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Upstream connection error: {}", e);
        }
    });

    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(ProxyProcessor {
        upstream_client: client,
    })));

    let server_addr = "127.0.0.1:5431";
    let listener = TcpListener::bind(server_addr).await?;
    println!("Listening on {}", server_addr);

    loop {
        let (socket, _) = listener.accept().await?;
        let processor_ref = processor.make();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, processor_ref.clone(), processor_ref).await {
                eprintln!("Error processing connection: {}", e);
            }
        });
    }
}