// type Callback = Box<dyn FnOnce(&mut Connection)
//

use crossbeam_channel::Sender;
use tokio::sync::oneshot;

use crate::{Connection, Result};

#[async_trait::async_trait]
pub trait Execute: Send + Sync {
    async fn call<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Connection) -> Result<R> + Send + 'static,
        R: Send + 'static;
}

type Cb = Box<dyn FnOnce(&Connection) + Send + 'static>;

pub(crate) struct Thread(Sender<Cb>);

impl Thread {
    pub(crate) fn new(conn: Connection) -> Result<Self> {
        let (tx, rx) = crossbeam_channel::bounded::<Cb>(128);
        std::thread::spawn(move || {
            while let Ok(cb) = rx.recv() {
                cb(&conn);
            }
        });

        Ok(Self(tx))
    }
}

#[async_trait::async_trait]
impl Execute for Thread {
    async fn call<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Connection) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let cb = Box::new(move |conn: &Connection| {
            let out = f(conn);
            let _ = tx.send(out);
        });

        self.0.send(cb).unwrap();

        rx.await.unwrap()
    }
}
