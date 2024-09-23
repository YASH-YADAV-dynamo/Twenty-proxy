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
        let database = login.database();

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
