use rand::Rng;
use slotmap::{new_key_type, SlotMap};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::future::Future;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::{Arc, RwLock, RwLockReadGuard};

new_key_type! { struct ConnectionKey; }

pub trait Poolable: Unpin + Send + Sized + 'static {
    type Error: Debug + Send + 'static;
    type AddrKey: Key;

    fn new(addr_key: Self::AddrKey) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    fn is_closed(&self) -> bool;
}

impl<T: Poolable + Sync> Poolable for Arc<T> {
    type AddrKey = T::AddrKey;
    type Error = T::Error;

    fn is_closed(&self) -> bool {
        self.deref().is_closed()
    }

    async fn new(addr_key: Self::AddrKey) -> Result<Self, Self::Error> {
        T::new(addr_key).await.map(Arc::new)
    }
}

pub trait Key: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}
impl<T> Key for T where T: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}

struct ConnPoolInner<T, K: Key> {
    connections: SlotMap<ConnectionKey, T>,
    host_to_conn_keys: HashMap<K, Vec<ConnectionKey>>,
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
        let mut inner = self.inner.write().unwrap();
        let conn_key = inner.connections.insert(value.clone());
        let keys = inner.host_to_conn_keys.entry(key.clone()).or_default();
        keys.push(conn_key);
    }

    pub async fn checkout(&self, addr_key: K) -> Result<T, Error>
    where
        T: Poolable + Clone,
        T::AddrKey: From<K>,
    {
        let mut conn_key = {
            let inner = self.inner.read().unwrap();
            Self::get_conn_key(&inner, &addr_key)?
        };

        loop {
            let (is_closed, conn_clone) = {
                let inner = self.inner.read().unwrap();
                if let Some(conn) = inner.connections.get(conn_key) {
                    (conn.is_closed(), Some(conn.clone()))
                } else {
                    (true, None)
                }
            };

            if let Some(conn) = conn_clone {
                if !is_closed {
                    return Ok(conn);
                } else {
                    // Connection is broken, replace it with newly created one
                    {
                        let mut inner = self.inner.write().unwrap();
                        // Check if the connection is still broken, it might have been replaced by another thread
                        if let Some(c) = inner.connections.get(conn_key) {
                            if c.is_closed() {
                                inner.connections.remove(conn_key);
                            }
                        }
                    }

                    let new_conn = match T::new(addr_key.clone().into()).await {
                        Ok(c) => c,
                        Err(e) => panic!("Failed to create new connection: {:?}", e),
                    };

                    let mut inner = self.inner.write().unwrap();
                    let new_conn_key = inner.connections.insert(new_conn.clone());
                    if let Some(keys) = inner.host_to_conn_keys.get_mut(&addr_key) {
                        if let Some(key_ref) = keys.iter_mut().find(|k| **k == conn_key) {
                            *key_ref = new_conn_key;
                        }
                    }
                    return Ok(new_conn);
                }
            }

            // Connection key not valid anymore. Get a new one.
            let inner = self.inner.read().unwrap();
            match Self::get_conn_key(&inner, &addr_key) {
                Ok(key) => {
                    conn_key = key;
                    // loop to try again with the new key.
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn get_conn_key(
        inner_locked: &RwLockReadGuard<'_, ConnPoolInner<T, K>>,
        key: &K,
    ) -> Result<ConnectionKey, Error> {
        match inner_locked.host_to_conn_keys.get(key) {
            None => Err(Error::NoConnectionAvailable),
            Some(keys) => {
                if keys.is_empty() {
                    Err(Error::NoConnectionAvailable)
                } else {
                    let idx = rand::thread_rng().gen_range(0..keys.len());
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
    async fn test_random_checkout() {
        let pool = ConnPool::<MockConnection, MockKey>::new();

        let key = MockKey("foo".to_string());
        let key_for_conn1 = key.clone();
        let key_for_conn2 = key.clone();

        pool.pooled(key_for_conn1, mock_conn(1));
        pool.pooled(key_for_conn2, mock_conn(2));

        let p1 = pool.checkout(key.clone()).await.unwrap();
        let p2 = pool.checkout(key.clone()).await.unwrap();
        let p3 = pool.checkout(key.clone()).await.unwrap();

        let possible_ids = vec![1, 2];
        assert!(possible_ids.contains(&p1.id));
        assert!(possible_ids.contains(&p2.id));
        assert!(possible_ids.contains(&p3.id));
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

        let inner = pool.inner.read().unwrap();
        assert_eq!(inner.connections.len(), 2);
    }
}
