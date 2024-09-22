use serde::Deserialize;
use std::collections::HashMap;

#[derive(Clone, Deserialize)]
pub struct ProxyConfig {
    pub listen_address: String,
    pub upstream_connection_string: String,
    pub allowed_schemas: Vec<String>,
    pub ip_whitelist: Vec<String>,
    pub opt_in_schemas: Vec<String>,
}

impl ProxyConfig {
    pub fn from_env() -> Result<Self, envy::Error> {
        envy::from_env::<ProxyConfig>()
    }
}