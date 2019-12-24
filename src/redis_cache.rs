use log::warn;
use futures::compat::Future01CompatExt;
use simple_redis::{client::Client as RedisClient, RedisError};

use async_trait::async_trait;

use crate::cache::{Cache, Cacheable, CacheResponse};

#[derive(Clone)]
pub struct RedisCache {
    address: String,
}

impl RedisCache {
    pub fn new(address: String) -> Result<Self, RedisError> {
        Ok(
            Self {
                address,
            }
        )
    }
}

#[async_trait]
impl Cache<String> for RedisCache {
    async fn get<CA>(&mut self, cacheable: CA) -> Result<CacheResponse, String>
        where
            CA: Cacheable + Send + Sync + 'static
    {
        let identity = cacheable.identity();
        let identity = hex::encode(identity);
//
        let res = simple_redis::create(&self.address)
            .map_err(|e| format!("{:?}", e))
            .and_then(|mut client| {
                client.exists(&identity)
                    .map_err(|e| format!("{:?}", e))
            });
        match res {
            Ok(true) => Ok(CacheResponse::Hit),
            Ok(false) => Ok(CacheResponse::Miss),
            Err(e) => {
                warn!("Cache lookup failed with: {:?}", e);
                Ok(CacheResponse::Miss)
            }
        }
    }

    async fn store(&mut self, identity: Vec<u8>) -> Result<(), String>
    {
        let identity = hex::encode(identity);

        simple_redis::create(&self.address)
            .map_err(|e| format!("{:?}", e))
            .and_then(|mut client| {
                client.set(&identity, true)
                    .map(|_| ())
                    .map_err(|e| format!("{:?}", e))
            })
    }
}