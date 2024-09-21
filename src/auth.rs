use std::sync::Arc;
use async_trait::async_trait;
use pgwire::api::auth::{AuthenticationHandler, AuthenticationSource, LoginInfo, Password, ServerParameterProvider};
use pgwire::error::{PgWireError, PgWireResult};
use crate::config::ProxyConfig;

pub struct AuthHandler {
    config: Arc<ProxyConfig>,
}

impl AuthHandler {
    pub fn new(config: Arc<ProxyConfig>) -> Self {
        Self { config }
    }
}

#[async_trait]
impl AuthenticationHandler for AuthHandler {
    async fn authenticate<C: ServerParameterProvider>(
        &self,
        login: &LoginInfo,
        _password: &Password,
        _source: &AuthenticationSource,
        _params: &mut C,
    ) -> PgWireResult<()> {
        // Implement actual authentication logic here
        // For this example, we're just checking if the requested schema is allowed
        if self.config.allowed_schemas.contains(&login.database) {
            Ok(())
        } else {
            Err(PgWireError::UserError(Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Schema not allowed",
            ))))
        }
    }
}