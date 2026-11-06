use std::{fs, net::SocketAddr, path::Path};

use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node_id: String,
    pub listen_address: SocketAddr,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| Error::ConfigRead {
            path: path.display().to_string(),
            source,
        })?;
        let config: Self = toml::from_str(&text)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        let valid_node_id = !self.node_id.is_empty()
            && self.node_id.len() <= 128
            && self
                .node_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte));
        if !valid_node_id {
            return Err(Error::ConfigValidation(
                "node_id must be 1-128 ASCII letters, digits, '.', '_' or '-'".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> Config {
        Config {
            node_id: "local-1".into(),
            listen_address: "127.0.0.1:9092".parse().unwrap(),
        }
    }

    #[test]
    fn rejects_invalid_node_id() {
        let mut config = valid();
        config.node_id = "not valid".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unknown_fields() {
        let error =
            toml::from_str::<Config>("node_id='n'\nlisten_address='127.0.0.1:1'\nextra=true");
        assert!(error.is_err());
    }
}
