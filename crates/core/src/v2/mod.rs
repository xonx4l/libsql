mod hrana;
mod rows;
mod statement;
mod transaction;

use std::sync::Arc;

use crate::{Params, Result, TransactionBehavior};
pub use hrana::{Client, HranaError};

pub use rows::{Row, Rows};
use statement::LibsqlStmt;
pub use statement::Statement;
use transaction::LibsqlTx;
pub use transaction::Transaction;

// TODO(lucio): Improve construction via
//      1) Move open errors into open fn rather than connect
//      2) Support replication setup
enum DbType {
    Memory,
    File { path: String },
    Sync { db: crate::Database },
    Remote { url: String, auth_token: String },
}
pub enum OpenFlags {
    SQLITE_RO,     
    SQLITE_RW,        
}

pub struct Database {
    db_type: DbType,
    flags: Option<OpenFlags>,
}

impl Database {
    pub fn open_in_memory() -> Result<Self> {
        Ok(Database {
            db_type: DbType::Memory,
        })
    }

    pub fn open(db_path: impl Into<String>) -> Result<Database> {
        Ok(Database {
            db_type: DbType::File {
                path: db_path.into(),
            },
        })
    }
    
    pub fn open_with_flags<S: Into<String>>(
        db_path: S,
        flags: OpenFlags,
    ) -> Result<Database> {
        let db_path = db_path.into();
        let db_type = DbType::File {
            path: db_path.clone(),
            readonly: flags == OpenFlags::SQLITE_RO,
        };

        Ok(Database {
            db_type,
            flags: Some(flags),
            })
         }
     }

    pub async fn open_with_sync(
        db_path: impl Into<String>,
        url: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Database> {
        let opts = crate::Opts::with_http_sync(url, token);
        let db = crate::Database::open_with_opts(db_path, opts).await?;
        Ok(Database {
            db_type: DbType::Sync { db },
        })
    }

    pub fn open_remote(url: impl Into<String>, auth_token: impl Into<String>) -> Result<Self> {
        Ok(Database {
            db_type: DbType::Remote {
                url: url.into(),
                auth_token: auth_token.into(),
            },
        })
    }

    pub async fn connect(&self) -> Result<Connection> {
        match &self.db_type {
            DbType::Memory => {
                let db = crate::Database::open_with_flags(":memory:",self.get_flags())?;
                let conn = db.connect()?;

                let conn = Arc::new(LibsqlConnection { conn });

                Ok(Connection { conn })
            }

            DbType::File { path } => {
                let db = crate::Database::open_with_flags(path,self.get_flags())?;
                let conn = db.connect()?;

                let conn = Arc::new(LibsqlConnection { conn });

                Ok(Connection { conn })
            }

            DbType::Sync { db } => {
                let conn = db.connect()?;

                let conn = Arc::new(LibsqlConnection { conn });

                Ok(Connection { conn })
            }

            DbType::Remote { url, auth_token } => {
                let conn = Arc::new(hrana::Client::new(url, auth_token));

                Ok(Connection { conn })
            }
        }
    }

    pub async fn sync(&self) -> Result<usize> {
        match &self.db_type {
            DbType::Sync { db } => db.sync().await,
            DbType::Memory => Err(crate::Error::SyncNotSupported("in-memory".into())),
            DbType::File { .. } => Err(crate::Error::SyncNotSupported("file".into())),
            DbType::Remote { .. }=> Err(crate::Error::SyncNotSupported("remote".into())),
        }
    }
}

#[async_trait::async_trait]
trait Conn {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64>;

    async fn execute_batch(&self, sql: &str) -> Result<()>;

    async fn prepare(&self, sql: &str) -> Result<Statement>;

    async fn transaction(&self, tx_behavior: TransactionBehavior) -> Result<Transaction>;

    fn is_autocommit(&self) -> bool;

    fn changes(&self) -> u64;

    fn last_insert_rowid(&self) -> i64;
}

#[derive(Clone)]
pub struct Connection {
    conn: Arc<dyn Conn + Send + Sync>,
}

// TODO(lucio): Convert to using tryinto params
impl Connection {
    pub async fn execute(&self, sql: &str, params: impl Into<Params>) -> Result<u64> {
        self.conn.execute(sql, params.into()).await
    }

    pub async fn execute_batch(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql).await
    }

    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        self.conn.prepare(sql).await
    }

    pub async fn query(&self, sql: &str, params: impl Into<Params>) -> Result<Rows> {
        let stmt = self.prepare(sql).await?;

        stmt.query(&params.into()).await
    }

    /// Begin a new transaction in DEFERRED mode, which is the default.
    pub async fn transaction(&self) -> Result<Transaction> {
        self.transaction_with_behavior(TransactionBehavior::Deferred)
            .await
    }

    /// Begin a new transaction in the given mode.
    pub async fn transaction_with_behavior(
        &self,
        tx_behavior: TransactionBehavior,
    ) -> Result<Transaction> {
        self.conn.transaction(tx_behavior).await
    }

    pub fn is_autocommit(&self) -> bool {
       self.conn.is_autocommit()
    }

    pub fn changes(&self) -> u64 {
       self.conn.changes()
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }
}

#[derive(Clone)]
struct LibsqlConnection {
    conn: crate::Connection,
}

#[async_trait::async_trait]
impl Conn for LibsqlConnection {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64> {
        self.conn.execute(sql, params)
    }

    async fn execute_batch(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql)
    }

    async fn prepare(&self, sql: &str) -> Result<Statement> {
        let sql = sql.to_string();

        let stmt = self.conn.prepare(sql)?;

        Ok(Statement {
            inner: Arc::new(LibsqlStmt(stmt)),
        })
    }

    async fn transaction(&self, tx_behavior: TransactionBehavior) -> Result<Transaction> {
        let tx = crate::Transaction::begin(self.conn.clone(), tx_behavior)?;
        // TODO(lucio): Can we just use the conn passed to the transaction?
        Ok(Transaction {
            inner: Box::new(LibsqlTx(Some(tx))),
            conn: Connection {
                conn: Arc::new(self.clone()),
            },
        })
    }

    fn is_autocommit(&self) -> bool {
       self.conn.is_autocommit()
    }

    fn changes(&self) -> u64 {
        self.conn.changes()
    }

    fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }
    fn get_flags(&self) -> OpenFlags {
        self.flags.unwrap_or(OpenFlags::SQLITE_RW)
    }
}
