use tokio::net::TcpListener;
use std::sync::Arc;
use pgwire::pg_server::{PgWireListener, BoxedClientTransmitter};
use pgwire::protocol::startup::{StartupPacket, ServerParameterProvider};
use std::collections::HashMap;
use std::net::SocketAddr;

struct CustomAuthProvider;

impl CustomAuthProvider {
    fn validate_credentials(&self, user: &str, password: &str) -> bool {
        
        true // This should be replaced with actual validation
    }

    fn is_ip_whitelisted(&self, addr: &SocketAddr) -> bool {
        
        true // Placeholder
    }

    fn log_connection_attempt(&self, user: &str, success: bool) {
        if success {
            log::info!("User {} connected successfully", user);
        } else {
            log::error!("Failed connection attempt for user {}", user);
        }
    }
    fn has_opted_in(&self, user: &str) -> bool {
        
        true // Replace with actual logic
    }
    fn authenticate_client(
        &self,
        startup_packet: &StartupPacket,
        transmitter: &BoxedClientTransmitter,
    ) -> pgwire::pg_response::PgResponse {
        let user = startup_packet.user().to_string();
        let password = startup_packet.password().unwrap_or_default().to_string();
        let client_addr = transmitter.remote_addr();

        if self.validate_credentials(&user, &password) && self.has_opted_in(&user) && self.is_ip_whitelisted(&client_addr) {
            self.log_connection_attempt(&user, true);
            pgwire::pg_response::PgResponse::empty() // Allow connection
        } else {
            self.log_connection_attempt(&user, false);
            transmitter
                .send_error_response(pgwire::protocol::error::ErrorSeverity::Fatal, "Authentication failed")
                .await;
            pgwire::pg_response::PgResponse::empty() // Deny connection
        }
    }
}


impl ServerParameterProvider for CustomAuthProvider {
    fn server_parameters(&self) -> HashMap<String, String> {
        let mut params = HashMap::new();
        params.insert("application_name".to_string(), "twenty-proxy".to_string());
        params
    }

    fn authenticate_client(
        &self,
        _startup_packet: &StartupPacket,
        _transmitter: &BoxedClientTransmitter,
    ) -> pgwire::pg_response::PgResponse {
        
        
        pgwire::pg_response::PgResponse::empty()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let listener = TcpListener::bind("0.0.0.0:5432").await?;
    let pg_server = PgWireListener::new(listener, Arc::new(CustomAuthProvider));
    pg_server.serve().await?;

    Ok(())
}

// Writng tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_credentials() {
        let provider = CustomAuthProvider {};
        assert!(provider.validate_credentials("valid_user", "valid_password"));
    }

    #[test]
    fn test_ip_whitelisting() {
        let provider = CustomAuthProvider {};
        assert!(provider.is_ip_whitelisted(&"127.0.0.1:5432".parse().unwrap()));
    }

    #[test]
    fn test_opt_in() {
        let provider = CustomAuthProvider {};
        assert!(provider.has_opted_in("valid_user"));
    }
}

