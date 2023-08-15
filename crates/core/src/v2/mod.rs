mod builder;
mod execute;
mod hrana;

use std::sync::Arc;

use crate::{Params, Result};
pub use hrana::{Client, HranaError};

enum DbType {
    Memory,
    Http { url: String },
}

pub struct Database {
    db_type: DbType,
}

impl Database {
    pub fn connect(&self) -> Result<Connection> {
        match &self.db_type {
            DbType::Memory => {
                let db = crate::Database::open(":memory:")?;
                let conn = db.connect()?;

                let e = execute::Thread::new(conn)?;

                let conn = Arc::new(LibsqlConnection { e });

                Ok(Connection { conn })
            }

            DbType::Http { url } => {
                todo!()
            }
        }
    }
}

#[async_trait::async_trait]
trait Conn {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64>;

    async fn prepare(&self, sql: &str) -> Result<Statement>;
}

pub struct Connection {
    conn: Arc<dyn Conn>,
}

impl Connection {
    pub async fn execute(&self, sql: &str, params: impl Into<Params>) -> Result<u64> {
        self.conn.execute(sql, params.into()).await
    }

    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        self.conn.prepare(sql).await
    }
}

pub struct Statement {
    conn: Arc<dyn Conn>,
    sql: String,
}

pub struct BatchResult {}

pub struct ResultSet {}

struct LibsqlConnection<E> {
    e: E,
}

#[async_trait::async_trait]
impl<E: execute::Execute> Conn for LibsqlConnection<E> {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64> {
        let sql = sql.to_string();
        self.e.call(move |conn| conn.execute(sql, params)).await
    }

    async fn prepare(&self, sql: &str) -> Result<Statement> {
        let sql = sql.to_string();

        let stmt = self.e.call(move |conn| conn.prepare(sql)).await;

        todo!()
    }
}

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
