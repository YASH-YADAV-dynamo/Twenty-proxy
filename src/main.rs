use std::sync::Arc;
use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
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
        self.upstream_client
            .simple_query(query)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))
            .map(|resp_msgs| {
                let mut downstream_response = Vec::new();
                let mut row_buf = Vec::new();
                let mut field_info: Vec<FieldInfo> = Vec::new();
                
                for resp in resp_msgs {
                    match resp {
                        SimpleQueryMessage::CommandComplete(count) => {
                            if !row_buf.is_empty() {
                                let query_response = QueryResponse::new(field_info.clone(), row_buf.clone());
                                downstream_response.push(Response::Query(query_response));
                                row_buf.clear();
                            }
                            downstream_response.push(Response::Execution(
                                Tag::new_for_execution("", Some(count as usize)),
                            ));
                        }
                        SimpleQueryMessage::Row(row) => {
                            if field_info.is_empty() {
                                // Initialize field info based on row
                                for (idx, column) in row.columns().iter().enumerate() {
                                    field_info.push(FieldInfo::new(
                                        column.name().to_string(),
                                        None,
                                        Type::VARCHAR, 
                                        FieldFormat::Text,
                                    ));
                                }
                            }
                            
                            let mut encoder = DataRowEncoder::new(field_info.len());
                            for value in row.iter() {
                                if let Some(val) = value {
                                    encoder.encode_text(val).unwrap();
                                } else {
                                    encoder.encode_text("").unwrap(); 
                                }
                            }
                            row_buf.push(encoder.finish());
                        }
                        _ => {}
                    }
                }

                downstream_response
            })
    }
}

// Main Function
#[tokio::main]
pub async fn main() {
    let (client, connection) = tokio_postgres::connect("host=127.0.0.1 user=postgres", NoTls)
        .await
        .expect("Cannot establish upstream connection");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Upstream connection error: {}", e);
        }
    });

    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(ProxyProcessor {
        upstream_client: client,
    })));

    // Placeholder for extended query handler
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

    let server_addr = "127.0.0.1:5431";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await
        });
    }
}
