use hrana_client_proto::pipeline::{
    ClientMsg, Response, ServerMsg, StreamBatchReq, StreamExecuteReq, StreamRequest,
    StreamResponse, StreamResponseError, StreamResponseOk,
};
use hrana_client_proto::{Batch, BatchResult, Stmt, StmtResult};

// use crate::client::Config;
use crate::{Params, Result};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::{Conn, Statement};

/// Information about the current session: the server-generated cookie
/// and the URL that should be used for further communication.
#[derive(Clone, Debug, Default)]
struct Cookie {
    baton: Option<String>,
    base_url: Option<String>,
}

/// Generic HTTP client. Needs a helper function that actually sends
/// the request.
#[derive(Clone, Debug)]
pub struct Client {
    inner: InnerClient,
    cookies: Arc<RwLock<HashMap<u64, Cookie>>>,
    url_for_queries: String,
    auth: String,
}

#[derive(Clone, Debug)]
pub struct InnerClient {}

impl InnerClient {
    pub async fn send(&self, url: String, auth: String, body: String) -> Result<ServerMsg> {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HranaError {
    #[error("missing environment variable: `{0}`")]
    MissingEnv(String),
    #[error("unexpected response: `{0}`")]
    UnexpectedResponse(String),
    #[error("stream closed: `{0}`")]
    StreamClosed(String),
    #[error("stream error: `{0:?}`")]
    StreamError(StreamResponseError),
    #[error("json error: `{0}`")]
    Json(#[from] serde_json::Error),
}

impl Client {
    /// Creates a database client with JWT authentication.
    ///
    /// # Arguments
    /// * `url` - URL of the database endpoint
    /// * `token` - auth token
    pub fn new(inner: InnerClient, url: impl Into<String>, token: impl Into<String>) -> Self {
        let token = token.into();
        let url = url.into();
        // Auto-update the URL to start with https:// if no protocol was specified
        let base_url = if !url.contains("://") {
            format!("https://{}", &url)
        } else {
            url
        };
        let url_for_queries = format!("{base_url}v2/pipeline");
        Self {
            inner,
            cookies: Arc::new(RwLock::new(HashMap::new())),
            url_for_queries,
            auth: format!("Bearer {token}"),
        }
    }

    // /// Establishes  a database client from a `Config` object
    // pub fn from_config(inner: InnerClient, config: Config) -> Result<Self> {
    //     Ok(Self::new(
    //         inner,
    //         config.url,
    //         config.auth_token.unwrap_or_default(),
    //     ))
    // }

    pub fn from_env(inner: InnerClient) -> Result<Client> {
        let url = std::env::var("LIBSQL_CLIENT_URL").map_err(|_| {
            HranaError::MissingEnv(
                "LIBSQL_CLIENT_URL variable should point to your sqld database".into(),
            )
        })?;

        let token = std::env::var("LIBSQL_CLIENT_TOKEN").unwrap_or_default();
        Ok(Client::new(inner, url, token))
    }
}

impl Client {
    pub async fn raw_batch(&self, stmts: impl IntoIterator<Item = Stmt>) -> Result<BatchResult> {
        let mut batch = Batch::new();
        for stmt in stmts.into_iter() {
            batch.step(None, stmt);
        }

        let msg = ClientMsg {
            baton: None,
            requests: vec![
                StreamRequest::Batch(StreamBatchReq { batch }),
                StreamRequest::Close,
            ],
        };
        let body = serde_json::to_string(&msg).map_err(HranaError::Json)?;
        let mut response: ServerMsg = self
            .inner
            .send(self.url_for_queries.clone(), self.auth.clone(), body)
            .await?;

        if response.results.is_empty() {
            Err(HranaError::UnexpectedResponse(format!(
                "Unexpected empty response from server: {:?}",
                response.results
            )))?;
        }
        if response.results.len() > 2 {
            // One with actual results, one closing the stream
            Err(HranaError::UnexpectedResponse(format!(
                "Unexpected multiple responses from server: {:?}",
                response.results
            )))?;
        }
        match response.results.swap_remove(0) {
            Response::Ok(StreamResponseOk {
                response: StreamResponse::Batch(batch_result),
            }) => Ok(batch_result.result),
            Response::Ok(_) => Err(HranaError::UnexpectedResponse(format!(
                "Unexpected response from server: {:?}",
                response.results
            ))
            .into()),
            Response::Error(e) => Err(HranaError::StreamError(e).into()),
        }
    }

    async fn execute_inner(&self, stmt: Stmt, tx_id: u64) -> Result<StmtResult> {
        let cookie = if tx_id > 0 {
            self.cookies
                .read()
                .unwrap()
                .get(&tx_id)
                .cloned()
                .unwrap_or_default()
        } else {
            Cookie::default()
        };
        let msg = ClientMsg {
            baton: cookie.baton,
            requests: vec![StreamRequest::Execute(StreamExecuteReq { stmt })],
        };
        let body = serde_json::to_string(&msg).map_err(HranaError::Json)?;
        let url = cookie
            .base_url
            .unwrap_or_else(|| self.url_for_queries.clone());
        let mut response: ServerMsg = self.inner.send(url, self.auth.clone(), body).await?;

        if tx_id > 0 {
            let base_url = response.base_url;
            match response.baton {
                Some(baton) => {
                    self.cookies.write().unwrap().insert(
                        tx_id,
                        Cookie {
                            baton: Some(baton),
                            base_url,
                        },
                    );
                }
                None => Err(HranaError::StreamClosed(
                    "Stream closed: server returned empty baton".into(),
                ))?,
            }
        }

        if response.results.is_empty() {
            Err(HranaError::UnexpectedResponse(format!(
                "Unexpected empty response from server: {:?}",
                response.results
            )))?;
        }
        if response.results.len() > 1 {
            // One with actual results, one closing the stream
            Err(HranaError::UnexpectedResponse(format!(
                "Unexpected multiple responses from server: {:?}",
                response.results
            )))?;
        }
        match response.results.swap_remove(0) {
            Response::Ok(StreamResponseOk {
                response: StreamResponse::Execute(execute_result),
            }) => Ok(execute_result.result),
            Response::Ok(_) => Err(HranaError::UnexpectedResponse(format!(
                "Unexpected response from server: {:?}",
                response.results
            ))
            .into()),
            Response::Error(e) => Err(HranaError::StreamError(e).into()),
        }
    }

    async fn close_stream_for(&self, tx_id: u64) -> Result<()> {
        let cookie = self
            .cookies
            .read()
            .unwrap()
            .get(&tx_id)
            .cloned()
            .unwrap_or_default();
        let msg = ClientMsg {
            baton: cookie.baton,
            requests: vec![StreamRequest::Close],
        };
        let url = cookie
            .base_url
            .unwrap_or_else(|| self.url_for_queries.clone());
        let body = serde_json::to_string(&msg).map_err(HranaError::Json)?;
        self.inner.send(url, self.auth.clone(), body).await.ok();
        self.cookies.write().unwrap().remove(&tx_id);
        Ok(())
    }

    // /// # Arguments
    // /// * `stmt` - the SQL statement
    // pub async fn execute(&self, stmt: impl Into<Statement> + Send) -> Result<ResultSet> {
    //     self.execute_inner(stmt, 0).await
    // }

    // pub async fn execute_in_transaction(&self, tx_id: u64, stmt: Statement) -> Result<ResultSet> {
    //     self.execute_inner(stmt, tx_id).await
    // }

    // pub async fn commit_transaction(&self, tx_id: u64) -> Result<()> {
    //     self.execute_inner("COMMIT", tx_id).await.map(|_| ())?;
    //     self.close_stream_for(tx_id).await.ok();
    //     Ok(())
    // }

    // pub async fn rollback_transaction(&self, tx_id: u64) -> Result<()> {
    //     self.execute_inner("ROLLBACK", tx_id).await.map(|_| ())?;
    //     self.close_stream_for(tx_id).await.ok();
    //     Ok(())
    // }
}

#[async_trait::async_trait]
impl Conn for Client {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64> {
        todo!()
    }

    async fn prepare(&self, sql: &str) -> Result<Statement> {
        todo!()
    }
}
