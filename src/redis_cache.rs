use log::warn;
use futures::compat::Future01CompatExt;
use darkredis::ConnectionPool;
use darkredis::Error as RedisError;

use async_trait::async_trait;

use crate::cache::{Cache, Cacheable, CacheResponse};
use std::time::Duration;

#[derive(Clone)]
pub struct RedisCache {
    address: String,
    connection_pool: ConnectionPool,
}

impl RedisCache {
    pub async fn new(address: String) -> Result<Self, RedisError> {
        let connection_pool = ConnectionPool::create(
            "127.0.0.1:6379".into(),
            None,
            num_cpus::get()
        ).await?;

        Ok(
            Self {
                connection_pool,
                address,
            }
        )
    }
}

#[async_trait]
impl Cache<Box<dyn std::error::Error>> for RedisCache {
    async fn get<CA>(&mut self, cacheable: CA) -> Result<CacheResponse, Box<dyn std::error::Error>>
        where
            CA: Cacheable + Send + Sync + 'static
    {
        let identity = cacheable.identity();
        let identity = hex::encode(identity);
//
        let mut client = self.connection_pool.get().await;

        let res = tokio::time::timeout(
            Duration::from_millis(200),
            client.exists(&identity)
        ).await;

        let res = match res {
            Ok(res) => res,
            Err(e) => {
                warn!("Cache lookup failed with: {:?}", e);
                return Ok(CacheResponse::Miss)
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

    async fn store(&mut self, identity: Vec<u8>) -> Result<(), Box<dyn std::error::Error>>
    {
        let identity = hex::encode(identity);

        let mut client = self.connection_pool.get().await;

        let res = tokio::time::timeout(
            Duration::from_millis(200),
            client.set(&identity, b"1")
        ).await;

        res??;



        Ok(())
    }
}