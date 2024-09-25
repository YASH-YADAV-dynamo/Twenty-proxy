use std::sync::Arc;
use async_trait::async_trait;
use pgwire::api::auth::{Authentication, LoginInfo, Password, ServerParameterProvider, NoTlsAuthServer};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::tcp::PgWireTcpServer;
use crate::config::ProxyConfig;

/// Authentication handler that implements custom authentication logic
pub struct AuthHandler {
    config: Arc<ProxyConfig>,
}

impl AuthHandler {
    /// Create a new AuthHandler with the given configuration
    pub fn new(config: Arc<ProxyConfig>) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Authentication for AuthHandler {
    /// Authenticate the user using the provided login info and password
    async fn authenticate(
        &self,
        login: &LoginInfo,
        password: &Password,
        _params: &mut dyn ServerParameterProvider,
    ) -> PgWireResult<()> {
        let database = login.database();  // Get the database/schema from login info

        // Check if the schema (workspace) is allowed and has opted-in for direct access
        if self.config.allowed_schemas.contains(database) && self.config.opt_in_schemas.contains(database) {
            // Schema is allowed and opted-in, so we allow the connection
            Ok(())
        } else if !self.config.allowed_schemas.contains(database) {
            // Schema is not in the allowed list
            Err(PgWireError::UserError(Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Schema not allowed",
            ))))
        } else {
            // Schema is allowed but has not opted-in
            Err(PgWireError::UserError(Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Schema has not opted-in for direct access",
            ))))
        }
    }
}

/// Run the PostgreSQL proxy server
///
/// This function initializes the server, sets up authentication, and starts
/// listening for incoming connections on the specified host and port.
pub async fn run_proxy(config: Arc<ProxyConfig>) -> PgWireResult<()> {
    // Create the authentication handler
    let auth_handler = AuthHandler::new(config.clone());

    // Create a PgWire server with no TLS and the custom authentication handler
    let auth_server = NoTlsAuthServer::new(auth_handler);

    // Create the TCP server and bind it to the specified host and port
    let server = PgWireTcpServer::new(auth_server);
    
    // Get the host and port from the configuration and format the address
    let addr = format!("{}:{}", config.host, config.port);
    
    // Start the server and listen for incoming connections
    println!("Starting proxy server at {}", addr);
    server.serve(addr).await?;
    
    Ok(())
}
