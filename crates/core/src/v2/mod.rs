mod hrana;

use crate::{Params, Result};
pub use hrana::{Client, HranaError};

pub struct Database {}

impl Database {
    pub fn connect(&self) -> Result<Connection> {
        todo!()
    }
}

#[async_trait::async_trait]
trait Conn {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64>;
}

pub struct Connection {
    conn: Box<dyn Conn>,
}

impl Connection {
    pub async fn execute(&self, sql: &str, params: impl Into<Params>) -> Result<u64> {
        self.conn.execute(sql, params.into()).await
    }
}

pub struct Statement {
    sql: String,
}

pub struct BatchResult {}

pub struct ResultSet {}

/// Configuration for the database client
#[derive(Debug)]
pub struct Config {
    pub url: String,
    pub auth_token: Option<String>,
}

impl Config {
    ///// Create a new [Config]
    ///// # Examples
    /////
    ///// ```
    ///// # async fn f() -> anyhow::Result<()> {
    ///// # use libsql_client::Config;
    ///// let config = Config::new("file:////tmp/example.db")?;
    ///// let db = libsql_client::Client::from_config(config).await.unwrap();
    ///// # Ok(())
    ///// # }
    ///// ```
    //pub fn new<T: TryInto<url::Url>>(url: T) -> Result<Self>
    //where
    //    <T as TryInto<url::Url>>::Error: std::fmt::Display,
    //{
    //    Ok(Self {
    //        url: url
    //            .try_into()
    //            .map_err(|e| anyhow::anyhow!("Failed to parse url: {}", e))?,
    //        auth_token: None,
    //    })
    //}

    /// Adds an authentication token to config
    /// # Examples
    ///
    /// ```
    /// # async fn f() -> anyhow::Result<()> {
    /// # use libsql_client::Config;
    /// let config = Config::new("https://example.com/db")?.with_auth_token("secret");
    /// let db = libsql_client::Client::from_config(config).await.unwrap();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }
}
