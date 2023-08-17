use crate::{errors, Connection, Statement, Error, Params, Result, Value};
use libsql_sys::ValueType;

use std::cell::RefCell;
use std::ffi::c_char;

/// Query result rows.
#[derive(Debug, Clone)]
pub struct Rows {
    pub(crate) stmt: Statement,
    pub(crate) err: RefCell<Option<i32>>,
}

unsafe impl Send for Rows {} // TODO: is this safe?

impl Rows {
    pub fn new(stmt: Statement) -> Rows {
        Rows {
            stmt,
            err: RefCell::new(None),
        }
    }

    pub fn next(&self) -> Result<Option<Row>> {
        let err = match self.err.take() {
            Some(err) => err,
            None => self.stmt.inner.step(),
        };
        match err as u32 {
            libsql_sys::ffi::SQLITE_OK => Ok(None),
            libsql_sys::ffi::SQLITE_DONE => Ok(None),
            libsql_sys::ffi::SQLITE_ROW => Ok(Some(Row {
                stmt: self.stmt.clone(),
            })),
            _ => Err(Error::FetchRowFailed(errors::error_from_code(err))),
        }
    }

    pub fn column_count(&self) -> i32 {
        self.stmt.inner.column_count()
    }

    pub fn column_name(&self, idx: i32) -> &str {
        self.stmt.inner.column_name(idx)
    }

    pub fn column_type(&self, idx: i32) -> i32 {
        self.stmt.inner.column_type(idx)
    }

    pub fn as_ref(&self) -> &Statement {
        &self.stmt
    }
}

pub struct RowsFuture {
    pub(crate) conn: Connection,
    pub(crate) sql: String,
    pub(crate) params: Params,
}

impl RowsFuture {
    pub fn wait(&mut self) -> Result<Option<Rows>> {
        futures::executor::block_on(self)
    }
}

impl futures::Future for RowsFuture {
    type Output = Result<Option<Rows>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let stmt = self.conn.prepare(&self.sql)?;
        let ret = stmt.query(&self.params)?;
        std::task::Poll::Ready(Ok(Some(ret)))
    }
}

pub struct Row {
    pub(crate) stmt: Statement,
}

impl Row {
    pub fn get<T>(&self, idx: i32) -> Result<T>
    where
        T: FromValue,
    {
        let val = self.stmt.inner.column_value(idx);
        T::from_sql(val)
    }

    pub fn get_value(&self, idx: i32) -> Result<Value> {
        let val = self.stmt.inner.column_value(idx);
        Ok(val.into())
    }

    pub fn column_type(&self, idx: i32) -> Result<ValueType> {
        let val = self.stmt.inner.column_type(idx);
        match val as u32 {
            libsql_sys::ffi::SQLITE_INTEGER => Ok(ValueType::Integer),
            libsql_sys::ffi::SQLITE_FLOAT => Ok(ValueType::Real),
            libsql_sys::ffi::SQLITE_BLOB => Ok(ValueType::Blob),
            libsql_sys::ffi::SQLITE_TEXT => Ok(ValueType::Text),
            libsql_sys::ffi::SQLITE_NULL => Ok(ValueType::Null),
            _ => Err(Error::UnknownColumnType(idx, val)),
        }
    }

    pub fn column_name(&self, idx: i32) -> &str {
        self.stmt.inner.column_name(idx)
    }

    pub fn get_ref(&self, idx: i32) -> Result<crate::params::ValueRef<'_>> {
        Ok(crate::Statement::value_ref(&self.stmt.inner, idx as usize))
    }
}

pub trait FromValue {
    fn from_sql(val: libsql_sys::Value) -> Result<Self>
    where
        Self: Sized;
}

impl FromValue for i32 {
    fn from_sql(val: libsql_sys::Value) -> Result<Self> {
        let ret = val.int();
        Ok(ret)
    }
}

impl FromValue for u32 {
    fn from_sql(val: libsql_sys::Value) -> Result<Self> {
        let ret = val.int() as u32;
        Ok(ret)
    }
}

impl FromValue for i64 {
    fn from_sql(val: libsql_sys::Value) -> Result<Self> {
        let ret = val.int64();
        Ok(ret)
    }
}

impl FromValue for u64 {
    fn from_sql(val: libsql_sys::Value) -> Result<Self> {
        let ret = val.int64() as u64;
        Ok(ret)
    }
}

impl FromValue for f64 {
    fn from_sql(val: libsql_sys::Value) -> Result<Self> {
        let ret = val.double();
        Ok(ret)
    }
}

impl FromValue for Vec<u8> {
    fn from_sql(val: libsql_sys::Value) -> Result<Self> {
        let ret = val.blob();
        if ret.is_null() {
            return Err(Error::NullValue);
        }
        let ret = unsafe { std::slice::from_raw_parts(ret as *const u8, val.bytes() as usize) };
        Ok(ret.to_vec())
    }
}

impl FromValue for String {
    fn from_sql(val: libsql_sys::Value) -> Result<Self> {
        let ret = val.text();
        if ret.is_null() {
            return Err(Error::NullValue);
        }
        let ret = unsafe { std::ffi::CStr::from_ptr(ret as *const c_char) };
        let ret = ret.to_str().unwrap();
        Ok(ret.to_string())
    }
}

impl FromValue for &[u8] {
    fn from_sql(val: libsql_sys::Value) -> Result<Self> {
        let ret = val.blob();
        if ret.is_null() {
            return Err(Error::NullValue);
        }
        let ret = unsafe { std::slice::from_raw_parts(ret as *const u8, val.bytes() as usize) };
        Ok(ret)
    }
}

impl FromValue for &str {
    fn from_sql(val: libsql_sys::Value) -> Result<Self> {
        let ret = val.text();
        if ret.is_null() {
            return Err(Error::NullValue);
        }
        let ret = unsafe { std::ffi::CStr::from_ptr(ret as *const c_char) };
        let ret = ret.to_str().unwrap();
        Ok(ret)
    }
}

pub struct MappedRows<F> {
    rows: Rows,
    map: F,
}

impl<F> MappedRows<F> {
    pub fn new(rows: Rows, map: F) -> Self {
        Self { rows, map }
    }
}

impl<F, T> Iterator for MappedRows<F>
where
    F: FnMut(Row) -> Result<T>,
{
    type Item = Result<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let map = &mut self.map;
        self.rows
            .next()
            .transpose()
            .map(|row_result| row_result.and_then(map))
    }
}
