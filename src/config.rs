use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct ProxyConfig {
    pub listen_address: String,
    pub upstream_connection_string: String,
    pub allowed_schemas: Vec<String>,
}

impl ProxyConfig {
    pub fn from_env() -> Result<Self, envy::Error> {
        envy::from_env::<ProxyConfig>()
    }
}