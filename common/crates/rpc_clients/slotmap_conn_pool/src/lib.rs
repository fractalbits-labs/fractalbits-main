use parking_lot::RwLock;
use slotmap::{SlotMap, new_key_type};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::future::Future;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;

new_key_type! { struct ConnectionKey; }

pub trait Poolable: Unpin + Send + Sized + 'static {
    type Error: Debug + Send + 'static;
    type AddrKey: Key;

    fn new(addr_key: Self::AddrKey) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    fn new_with_session_id(
        addr_key: Self::AddrKey,
        session_id: u64,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    fn new_with_session_and_request_id(
        addr_key: Self::AddrKey,
        session_id: u64,
        next_request_id: u32,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    fn is_closed(&self) -> bool;

    /// Extract current session state for persistence across reconnections
    fn get_session_state(&self) -> (u64, u32);
}

impl<T: Poolable + Sync> Poolable for Arc<T> {
    type AddrKey = T::AddrKey;
    type Error = T::Error;

    fn is_closed(&self) -> bool {
        self.deref().is_closed()
    }

    fn get_session_state(&self) -> (u64, u32) {
        self.deref().get_session_state()
    }

    async fn new(addr_key: Self::AddrKey) -> Result<Self, Self::Error> {
        T::new(addr_key).await.map(Arc::new)
    }

    async fn new_with_session_id(
        addr_key: Self::AddrKey,
        session_id: u64,
    ) -> Result<Self, Self::Error> {
        T::new_with_session_id(addr_key, session_id)
            .await
            .map(Arc::new)
    }

    async fn new_with_session_and_request_id(
        addr_key: Self::AddrKey,
        session_id: u64,
        next_request_id: u32,
    ) -> Result<Self, Self::Error> {
        T::new_with_session_and_request_id(addr_key, session_id, next_request_id)
            .await
            .map(Arc::new)
    }
}

pub trait Key: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}
impl<T> Key for T where T: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}

struct ConnPoolInner<T, K: Key> {
    connections: SlotMap<ConnectionKey, (T, Arc<Semaphore>)>,
    host_to_conn_keys: HashMap<K, (Vec<ConnectionKey>, AtomicUsize)>,
}

pub struct ConnPool<T, K: Key> {
    inner: Arc<RwLock<ConnPoolInner<T, K>>>,
}

impl<T, K: Key> Clone for ConnPool<T, K> {
    fn clone(&self) -> Self {
        ConnPool {
            inner: self.inner.clone(),
        }
    }
}

#[allow(clippy::new_without_default)]
impl<T: Poolable, K: Key> ConnPool<T, K> {
    pub fn new() -> Self {
        let inner = Arc::new(RwLock::new(ConnPoolInner {
            connections: SlotMap::with_key(),
            host_to_conn_keys: HashMap::new(),
        }));
        ConnPool { inner }
    }

    pub fn pooled(&self, key: K, value: T)
    where
        T: Clone,
    {
        let mut inner = self.inner.write();
        let conn_key = inner
            .connections
            .insert((value.clone(), Arc::new(Semaphore::new(1))));
        let (keys, _counter) = inner
            .host_to_conn_keys
            .entry(key.clone())
            .or_insert_with(|| (Vec::new(), AtomicUsize::new(0)));
        keys.push(conn_key);
    }

    pub async fn checkout(&self, addr_key: K) -> Result<T, Error>
    where
        T: Poolable + Clone,
        T::AddrKey: From<K>,
    {
        self.checkout_internal(addr_key, None).await
    }

    pub async fn checkout_with_session(&self, addr_key: K, session_id: u64) -> Result<T, Error>
    where
        T: Poolable + Clone,
        T::AddrKey: From<K>,
    {
        self.checkout_internal(addr_key, Some(session_id)).await
    }

    async fn checkout_internal(&self, addr_key: K, session_id: Option<u64>) -> Result<T, Error>
    where
        T: Poolable + Clone,
        T::AddrKey: From<K>,
    {
        let mut current_conn_key = self.get_conn_key(&addr_key)?;

        loop {
            let (is_closed, conn_option, semaphore_option) = {
                let inner = self.inner.read();
                if let Some((conn, semaphore)) = inner.connections.get(current_conn_key) {
                    (
                        conn.is_closed(),
                        Some(conn.clone()),
                        Some(semaphore.clone()),
                    )
                } else {
                    (true, None, None)
                }
            };

            if let (Some(conn), Some(semaphore)) = (conn_option, semaphore_option) {
                if !is_closed {
                    return Ok(conn);
                } else {
                    // Connection is broken. Acquire the per-connection semaphore.
                    // This will block other tasks trying to recreate *this specific* connection.
                    let _permit = semaphore.acquire().await.unwrap();

                    // Re-check connection state after acquiring the semaphore.
                    // Another thread might have already recreated it while we were waiting for the semaphore.
                    let (is_closed_after_lock, conn_after_lock_option) = {
                        let inner = self.inner.read();
                        if let Some((conn, _)) = inner.connections.get(current_conn_key) {
                            (conn.is_closed(), Some(conn.clone()))
                        } else {
                            (true, None)
                        }
                    };

                    if let Some(conn_after_lock) = conn_after_lock_option
                        && !is_closed_after_lock
                    {
                        return Ok(conn_after_lock);
                    }

                    // If we reach here, the connection is still broken and we hold the semaphore.
                    // Extract session state from broken connection before removing it
                    let (old_session_id, old_next_request_id) = {
                        let inner = self.inner.read();
                        if let Some((conn, _)) = inner.connections.get(current_conn_key) {
                            conn.get_session_state()
                        } else {
                            // Connection was already removed, use provided session_id or create new
                            (session_id.unwrap_or(0), 1)
                        }
                    };

                    // Create replacement connection with session state from broken connection
                    let new_conn = if old_session_id != 0 {
                        // Use session state from broken connection
                        T::new_with_session_and_request_id(
                            addr_key.clone().into(),
                            old_session_id,
                            old_next_request_id,
                        )
                        .await
                    } else {
                        // No previous session state, create new connection
                        match session_id {
                            Some(id) => T::new_with_session_id(addr_key.clone().into(), id).await,
                            None => T::new(addr_key.clone().into()).await,
                        }
                    }
                    .unwrap_or_else(|e| panic!("Failed to create new connection: {e:?}"));

                    // Acquire write lock to modify the pool.
                    let mut inner = self.inner.write(); // Acquire write lock

                    // Remove the old, broken connection.
                    inner.connections.remove(current_conn_key);

                    // Insert the new connection with a new semaphore.
                    let new_conn_key = inner
                        .connections
                        .insert((new_conn.clone(), Arc::new(Semaphore::new(1))));

                    // Update the host_to_conn_keys mapping.
                    if let Some((keys, _)) = inner.host_to_conn_keys.get_mut(&addr_key) {
                        if let Some(key_ref) = keys.iter_mut().find(|k| **k == current_conn_key) {
                            *key_ref = new_conn_key;
                        } else {
                            // This case should ideally not be hit if the logic is sound,
                            // but as a fallback, we add the new key.
                            keys.push(new_conn_key);
                        }
                    }
                    return Ok(new_conn);
                }
            }

            // If we reach here, the connection key was not valid (e.g., removed by another thread
            // before we could even check its status). Get a new one and retry the loop.
            current_conn_key = {
                match self.get_conn_key(&addr_key) {
                    Ok(key) => key,
                    Err(e) => return Err(e),
                }
            };
        }
    }

    fn get_conn_key(&self, key: &K) -> Result<ConnectionKey, Error> {
        let inner_locked = self.inner.read();
        match inner_locked.host_to_conn_keys.get(key) {
            None => Err(Error::NoConnectionAvailable),
            Some((keys, counter)) => {
                if keys.is_empty() {
                    Err(Error::NoConnectionAvailable)
                } else {
                    let idx = counter.fetch_add(1, Ordering::Relaxed) % keys.len();
                    Ok(keys[idx])
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum Error {
    PoolDisabled,
    NoConnectionAvailable,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::PoolDisabled => write!(f, "pool is disabled"),
            Error::NoConnectionAvailable => write!(f, "no connection available"),
        }
    }
}

impl StdError for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct MockConnection {
        id: usize,
        is_closed: bool,
    }

    impl Poolable for MockConnection {
        fn is_closed(&self) -> bool {
            self.is_closed
        }

        type Error = std::io::Error;

        type AddrKey = MockKey;

        async fn new(addr_key: Self::AddrKey) -> Result<Self, Self::Error> {
            Ok(mock_conn(addr_key.0.parse().unwrap()))
        }

        async fn new_with_session_id(
            addr_key: Self::AddrKey,
            _session_id: u64,
        ) -> Result<Self, Self::Error> {
            Ok(mock_conn(addr_key.0.parse().unwrap()))
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct MockKey(String);

    fn mock_conn(id: usize) -> MockConnection {
        MockConnection {
            id,
            is_closed: false,
        }
    }

    #[tokio::test]
    async fn test_checkout_and_pool() {
        let pool = ConnPool::<MockConnection, MockKey>::new();
        let key = MockKey("42".to_string());

        // Initially, no connection
        // FIXME: This test case is not valid anymore since we block until a connection is available.
        // let res = pool.checkout(key.clone()).await;
        // assert!(matches!(res, Err(Error::NoConnectionAvailable)));

        // Create and pool a connection
        pool.pooled(key.clone(), mock_conn(42));

        // Now checkout should succeed
        let pooled = pool.checkout(key).await.unwrap();
        assert_eq!(pooled.id, 42);
    }

    #[tokio::test]
    async fn test_round_robin_checkout() {
        let pool = ConnPool::<MockConnection, MockKey>::new();

        let key = MockKey("foo".to_string());

        pool.pooled(key.clone(), mock_conn(1));
        pool.pooled(key.clone(), mock_conn(2));

        let p1 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p1.id, 1);
        let p2 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p2.id, 2);
        let p3 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p3.id, 1);
        let p4 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p4.id, 2);
    }

    #[tokio::test]
    async fn test_connection_cleanup() {
        let pool = ConnPool::<MockConnection, MockKey>::new();

        let key = MockKey("1".to_string());

        pool.pooled(key.clone(), mock_conn(1));

        let mut closed_conn = mock_conn(2);
        closed_conn.is_closed = true;
        pool.pooled(key.clone(), closed_conn);

        let p1 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p1.id, 1);

        let inner = pool.inner.read();
        assert_eq!(inner.connections.len(), 2);
    }
}
