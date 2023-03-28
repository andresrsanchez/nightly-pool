use async_channel::{Receiver, Sender, TryRecvError};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Weak,
};
use std::time::{Instant, SystemTime};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};

#[tokio::main]
async fn main() {
    let pool = Pool::new();
    pool.acquire().await.unwrap();
}
struct Pool {
    inner_pool: Arc<InnerPool>,
}
pub struct Config {
    /// The maximum time before closing the idle connection from the pool
    /// if the number of current connections is greater than min_pool_size.
    /// Defaults to 5 minutes.
    pub conn_idle_lifetime: Duration,
    /// The maximum time to wait for a new connection for the pool.
    /// Defaults to 30 seconds.
    pub wait_for_conn_timeout: Duration,
    /// The maximum number of connections in the pool.
    /// Defaults to 100 connections.
    pub max_size: usize,
    /// The minimum number of connections in the pool.
    /// Defaults to 0 connections.
    pub min_size: usize,
    // The interval to clean the old idle connections in the pool.
    // Defaults to 10 seconds
    pub conn_idle_clean_interval: Duration,
}
impl Default for Config {
    fn default() -> Self {
        return Self {
            conn_idle_lifetime: Duration::from_secs(300),
            wait_for_conn_timeout: Duration::from_secs(30),
            max_size: 100,
            min_size: 0,
            conn_idle_clean_interval: Duration::from_secs(10),
        };
    }
}
#[derive(Debug)]
enum PoolError {
    CleaningProcessAlreadyInProgress,
    Generic,
}
struct InnerPool {
    idle_sender: Sender<Option<InnerPoolClient>>,
    idle_receiver: Receiver<Option<InnerPoolClient>>,
    sem: Semaphore,
    config: Config,
    cleaning: AtomicBool,
    clean_counter: AtomicUsize,
    idle: AtomicUsize,
}
impl Pool {
    pub fn new() -> Self {
        return Pool::new_with_config(Config::default());
    }
    pub fn new_with_config(config: Config) -> Self {
        let (tx, rx) = async_channel::unbounded();
        let size = config.max_size;
        let interval = config.conn_idle_clean_interval;
        let lil = Arc::new(InnerPool {
            idle_sender: tx,
            idle_receiver: rx,
            sem: Semaphore::new(size),
            config,
            cleaning: AtomicBool::new(false),
            clean_counter: AtomicUsize::new(0),
            idle: AtomicUsize::new(0),
        });
        let s = Arc::downgrade(&lil);
        let _ = tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                interval.tick().await;
                let _ = Pool::clean_pool(&s);
            }
        });
        return Self { inner_pool: lil };
    }
    pub fn clean_pool(shared: &Weak<InnerPool>) -> Result<(), PoolError> {
        let inner_pool = match shared.upgrade() {
            Some(s) => s,
            None => {
                return Err(PoolError::Generic);
            }
        };
        if let Err(_) =
            inner_pool
                .cleaning
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        {
            return Err(PoolError::CleaningProcessAlreadyInProgress);
        }
        inner_pool.clean_counter.fetch_add(1, Ordering::SeqCst);
        loop {
            if inner_pool.idle.load(Ordering::Relaxed) <= inner_pool.config.min_size {
                break;
            }
            match inner_pool.idle_receiver.try_recv() {
                Ok(Some(c)) => {
                    c.inner_conn.abort();
                    inner_pool.idle.fetch_sub(1, Ordering::SeqCst);
                    inner_pool.sem.add_permits(1); //review
                }
                Ok(None) => {
                    println!("NONE");
                    continue;
                }
                Err(_) => {
                    print!("errempty");
                    break;
                }
            }
        }
        inner_pool.cleaning.store(false, Ordering::SeqCst);
        return Ok(());
    }
    fn check_idle_client(&self, client: &InnerPoolClient) -> bool {
        if let Ok(elapsed) = client.started_at.elapsed() {
            return self.inner_pool.config.conn_idle_lifetime >= elapsed; //only this condition for now
        }
        return false;
    }
    pub async fn acquire(&self) -> Result<PoolClient, PoolError> {
        match self.inner_pool.idle_receiver.try_recv() {
            Ok(Some(c)) => {
                if self.check_idle_client(&c) {
                    return Ok(PoolClient { inner: Some(c) });
                }
                c.inner_conn.abort();
                self.inner_pool.sem.add_permits(1);
                self.inner_pool.idle.fetch_sub(1, Ordering::SeqCst);
                self.inner_pool.idle_sender.try_send(None);
            }
            Ok(None) => {
                if let Ok(guard) = self.inner_pool.sem.try_acquire() {
                    if let Ok(c) = InnerPoolClient::new(Arc::downgrade(&self.inner_pool)).await {
                        guard.forget();
                        return Ok(PoolClient { inner: Some(c) });
                    }
                }
            } //can create a new client
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Closed) => {} //channel closed?
        }
        let start = Instant::now();
        while start.elapsed() < self.inner_pool.config.wait_for_conn_timeout {
            if let Ok(guard) = self.inner_pool.sem.try_acquire() {
                if let Ok(c) = InnerPoolClient::new(Arc::downgrade(&self.inner_pool)).await {
                    guard.forget();
                    return Ok(PoolClient { inner: Some(c) });
                }
            }
            let maybe_msg = timeout(
                self.inner_pool.config.wait_for_conn_timeout,
                self.inner_pool.idle_receiver.recv(),
            )
            .await;
            match maybe_msg {
                Ok(Ok(Some(msg))) => {
                    if self.check_idle_client(&msg) {
                        return Ok(PoolClient { inner: Some(msg) });
                    }
                    msg.inner_conn.abort();
                    self.inner_pool.sem.add_permits(1);
                    self.inner_pool.idle.fetch_sub(1, Ordering::SeqCst);
                    self.inner_pool.idle_sender.try_send(None);
                }
                Ok(_) => {}
                Err(_) => {}
            };
        }
        return Err(PoolError::Generic);
    }
    fn release(&self, client: InnerPoolClient) {
        match self.inner_pool.idle_sender.try_send(Some(client)) {
            Ok(()) => {
                self.inner_pool.idle.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => println!("error"),
        }
    }
    pub fn metrics(&self) -> Result<Metrics, PoolError> {
        let idle = self.inner_pool.idle.load(Ordering::Relaxed);
        let total = self.inner_pool.config.max_size - self.inner_pool.sem.available_permits();
        return Ok(Metrics {
            idle,
            busy: 0,
            total,
            clean_counter: self.inner_pool.clean_counter.load(Ordering::Relaxed),
        });
    }
}

#[derive(Clone)]
struct Metrics {
    pub idle: usize,
    pub busy: usize,
    pub total: usize,
    pub clean_counter: usize,
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
        let start = Instant::now();
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
        let duration = start.elapsed();
        println!("Time elapsed 1 {:?}", duration);
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
            } else {
                println!("cannot upgrade client on drop");
            }
        } else {
            println!("cannot upgrade client on drop");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Config, Pool};
    use std::time::{Duration, Instant};

    //#[tokio::test]
    async fn get_client_with_timeout_from_empty_pool() {
        let pool = Pool::new_with_config(Config {
            conn_idle_lifetime: Duration::from_secs(30),
            wait_for_conn_timeout: Duration::from_secs(1),
            max_size: 1,
            min_size: 0,
            conn_idle_clean_interval: Duration::from_secs(30),
        });
        {
            assert!(pool.acquire().await.is_ok());
            let start = Instant::now();
            let c = pool.acquire().await;
            assert!(start.elapsed() > Duration::from_secs(1));
            assert!(c.is_err());
        }
        let metrics = pool.metrics().unwrap();
        assert_eq!(metrics.total, 1);
        assert_eq!(metrics.idle, 1);
        assert!(pool.acquire().await.is_ok());
    }
    #[tokio::test]
    async fn get_client_and_open_with_timeout() {}
    #[tokio::test]
    async fn client_returned_to_the_pool_is_reset() {}
    #[tokio::test]
    async fn check_clients_pruning() {
        let i = 100;
        let pool = Pool::new_with_config(Config {
            conn_idle_lifetime: Duration::from_millis(10),
            wait_for_conn_timeout: Duration::from_secs(1),
            max_size: i,
            min_size: 0,
            conn_idle_clean_interval: Duration::from_millis(10),
        });
        {
            let mut vec = Vec::new();
            for _ in 0..i - 2 {
                let conn = pool.acquire().await;
                assert!(conn.is_ok());
                vec.push(conn);
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let metrics = pool.metrics().unwrap();
        assert_eq!(metrics.idle, 0);
    }
    #[tokio::test]
    async fn old_client_gets_pruned() {}
}
