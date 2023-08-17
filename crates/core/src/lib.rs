//! # libSQL API for Rust
//!
//! libSQL is an embeddable SQL database engine based on SQLite.
//! This Rust API is a batteries-included wrapper around the SQLite C API to support transparent replication while retaining compatibility with the SQLite ecosystem, such as the SQL dialect and extensions. If you are building an application in Rust, this is the crate you should use.
//! There are also libSQL language bindings of this Rust crate to other languages such as [JavaScript](), Python, Go, and C.
//!
//! ## Getting Started
//!
//! To get started, you first need to create a [`Database`] object and then open a [`Connection`] to it, which you use to query:
//!
//! ```rust,no_run
//! use libsql::Database;
//!
//! let db = Database::open(":memory:").unwrap();
//! let conn = db.connect().unwrap();
//! conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ()) .unwrap();
//! conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')", ()).unwrap();
//! ```
//!
//! ## Embedded Replicas
//!
//! Embedded replica is libSQL database that's running in your application process, which keeps a local copy of a remote database.
//! They are useful if you want to move data in the memory space of your application for fast access.
//!
//! You can open an embedded read-only replica by using the [`Database::with_replicator`] constructor:
//!
//! ```rust,no_run
//! # async fn run() {
//! use libsql::{Database, Opts};
//! use libsql_replication::{Frame, Frames, Replicator};
//!
//! let mut db = Database::open_with_opts("/tmp/test.db", Opts::with_sync()).await.unwrap();
//!
//! let frames = Frames::Vec(vec![]);
//! db.sync_frames(frames).unwrap();
//! let conn = db.connect().unwrap();
//! conn.execute("SELECT * FROM users", ()).unwrap();
//! # }
//! ```
//!
//! ## Examples
//!
//! You can find more examples in the [`examples`](https://github.com/penberg/libsql-experimental/tree/libsql-api/crates/core/examples) directory.

pub mod connection;
pub mod database;
pub mod errors;
pub mod params;
pub mod rows;
pub mod statement;
pub mod transaction;
pub mod v2;

pub type Result<T> = std::result::Result<T, errors::Error>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub use libsql_sys::ffi;
pub use libsql_sys::ValueType;

pub use connection::Connection;
pub use database::Database;
#[cfg(feature = "replication")]
pub use database::Opts;
pub use errors::Error;
pub use params::Params;
pub use params::{params_from_iter, Value, ValueRef};
pub use rows::Row;
pub use rows::Rows;
pub use rows::RowsFuture;
pub use statement::{Column, Statement};
pub use transaction::{Transaction, TransactionBehavior};

/// Return the version of the underlying SQLite library as a number.
pub fn version_number() -> i32 {
    unsafe { ffi::sqlite3_libversion_number() }
}

/// Return the version of the underlying SQLite library as a string.
pub fn version() -> &'static str {
    unsafe {
        std::ffi::CStr::from_ptr(ffi::sqlite3_libversion())
            .to_str()
            .unwrap()
    }
}

