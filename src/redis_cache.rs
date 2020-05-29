use std::fmt::Debug;
use std::time::Duration;

use darkredis::Connection;
use darkredis::ConnectionPool;
use darkredis::Error as RedisError;
use futures::compat::Future01CompatExt;
use log::warn;

use async_trait::async_trait;

use crate::cache::{Cache, CacheResponse, Cacheable};

#[derive(Clone)]
pub struct RedisCache {
    address: String,
    connection_pool: ConnectionPool,
}

impl RedisCache {
    pub async fn new(address: String) -> Result<Self, RedisError> {
        let connection_pool =
            ConnectionPool::create(address.clone(), None, num_cpus::get()).await?;

        Ok(Self {
            connection_pool,
            address,
        })
    }
}

#[async_trait]
impl<E> Cache<E> for RedisCache
where
    E: Debug + Clone + Send + Sync + 'static,
{
    async fn get<CA>(&mut self, cacheable: CA) -> Result<CacheResponse, crate::error::Error<E>>
    where
        CA: Cacheable + Send + Sync + 'static,
    {
        let identity = cacheable.identity();
        let identity = hex::encode(identity);
        //
        let mut client = self.connection_pool.get().await;

        let res = tokio::time::timeout(Duration::from_millis(200), client.exists(&identity)).await;

        let res = match res {
            Ok(res) => res,
            Err(e) => {
                warn!("Cache lookup failed with: {:?}", e);
                return Ok(CacheResponse::Miss);
            }
        };

        match res {
            Ok(true) => Ok(CacheResponse::Hit),
            Ok(false) => Ok(CacheResponse::Miss),
            Err(e) => {
                warn!("Cache lookup failed with: {:?}", e);
                Ok(CacheResponse::Miss)
            }
        }
    }

    async fn store(&mut self, identity: Vec<u8>) -> Result<(), crate::error::Error<E>> {
        let identity = hex::encode(identity);

        let mut client = self.connection_pool.get().await;

        let res = tokio::time::timeout(
            Duration::from_millis(200),
            client.set_and_expire_seconds(&identity, b"1", 16 * 60),
        )
        .await;

        res.map_err(|err| crate::error::Error::CacheError(format!("{}", err)))?
            .map_err(|err| crate::error::Error::CacheError(format!("{}", err)))?;

        Ok(())
    }
}
