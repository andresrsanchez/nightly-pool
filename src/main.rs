use async_channel::{Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::sync::Weak;
use std::time::{Instant, SystemTime};
use std::vec::Vec;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use tokio_postgres::GenericClient;

#[tokio::main]
async fn main() {
    let pool = Pool::new(10);
    pool.acquire().await.unwrap();
}
struct Pool {
    inner_pool: Arc<InnerPool>,
}
struct InnerPool {
    //clients: Mutex<Vec<JoinHandle<()>>>, //include sempahore?
    idle_sender: Sender<Option<InnerPoolClient>>,
    idle_receiver: Receiver<Option<InnerPoolClient>>,
    max_size: usize,
    timeout: Duration,
    idle_lifetime: Duration,
    sem: Semaphore,
}
impl Pool {
    pub fn new(max_size: usize) -> Self {
        let (tx, rx) = async_channel::unbounded();
        return Self {
            inner_pool: Arc::new(InnerPool {
                //clients: Mutex::new(Vec::new()),
                idle_sender: tx,
                idle_receiver: rx,
                max_size,
                timeout: Duration::from_secs(30),
                idle_lifetime: Duration::from_secs(10),
                sem: Semaphore::new(max_size),
            }),
        };
    }
    fn check_idle_client(&self, client: &InnerPoolClient) -> bool {
        if let Ok(elapsed) = client.started_at.elapsed() {
            return self.inner_pool.idle_lifetime >= elapsed; //only this condition for now
        }
        return false;
    }
    //handle channel close and pool clear
    pub async fn acquire(&self) -> Result<PoolClient, tokio_postgres::Error> {
        match self.inner_pool.idle_receiver.try_recv() {
            Ok(Some(c)) => {} //check valid client
            Ok(None) => {}    //can create a new client
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Closed) => {}
        }
        let start = Instant::now();
        while start.elapsed() < self.inner_pool.timeout {
            if let Ok(guard) = self.inner_pool.sem.try_acquire() {
                match InnerPoolClient::new(Arc::downgrade(&self.inner_pool)).await {
                    Ok(c) => {
                        guard.forget();
                        return Ok(PoolClient { inner: Some(c) });
                    }
                    Err(_) => {}
                };
            }
            let maybe_msg = timeout(
                Duration::from_secs(30),
                self.inner_pool.idle_receiver.recv(),
            )
            .await;
            match maybe_msg {
                Ok(Ok(Some(msg))) => return Ok(PoolClient { inner: Some(msg) }),
                Ok(Ok(None)) => continue,
                Ok(_) => todo!(),
                Err(_) => todo!(),
            };
        }
        todo!()
    }
    fn release(&self, client: InnerPoolClient) {
        match self.inner_pool.idle_sender.try_send(Some(client)) {
            //or try_send?
            Ok(()) => println!("sended"),
            Err(_) => println!("error"),
        }
    }
    pub fn metrics(&self) -> usize {
        return self.inner_pool.sem.available_permits();
    }
}
struct PoolClient {
    inner: Option<InnerPoolClient>,
}
struct InnerPoolClient {
    pool: Weak<InnerPool>,
    inner_client: tokio_postgres::Client,
    inner_conn: JoinHandle<()>,
    started_at: SystemTime,
}
impl InnerPoolClient {
    async fn new(pool: Weak<InnerPool>) -> Result<Self, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::connect(
            "postgres://testing:testing@localhost:5433/testing",
            tokio_postgres::NoTls,
        )
        .await?;
        //timeout here¿?
        let conn = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        return Ok(Self {
            inner_client: client,
            inner_conn: conn,
            pool,
            started_at: SystemTime::now(),
        });
    }
}
impl Drop for PoolClient {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            if let Some(inner_pool) = inner.pool.upgrade() {
                let pool = Pool { inner_pool };
                pool.release(inner);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Pool;

    #[tokio::test]
    async fn get_client_from_empty_pool() {
        let pool = Pool::new(1);
        {
            pool.acquire().await.unwrap();
        }
        assert_eq!(pool.metrics(), 0);
    }
    #[tokio::test]
    async fn get_client_with_timeout_from_empty_pool() {
    }
    #[tokio::test]
    async fn get_client_and_open_with_timeout() {
    }
    #[tokio::test]
    async fn client_returned_to_the_pool_is_reset() {
    }
    #[tokio::test]
    async fn check_clients_pruning() {
    }
    #[tokio::test]
    async fn old_client_gets_pruned() { 
    }
}
