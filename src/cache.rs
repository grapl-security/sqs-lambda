use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use async_trait::async_trait;
use std::fmt::Debug;

pub trait Cacheable {
    fn identity(&self) -> Vec<u8>;
}

impl<H> Cacheable for H
    where
        H: Hash
{
    fn identity(&self) -> Vec<u8> {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        let hash = hasher.finish();
        hash.to_le_bytes().to_vec()
    }
}

#[derive(Clone)]
pub enum CacheResponse {
    Hit,
    Miss,
}

#[async_trait]
pub trait Cache<E>
    where
        E: Debug + Clone + Send + Sync + 'static,
{
    async fn get<CA: Cacheable + Send + Sync + 'static>
            (&mut self, cacheable: CA) -> Result<CacheResponse, crate::error::Error<E>>;
    async fn store(&mut self, identity: Vec<u8>) -> Result<(), crate::error::Error<E>>;
}

#[async_trait]
pub trait ReadableCache<E>
    where
        E: Debug + Clone + Send + Sync + 'static,
{
    async fn get<CA: Cacheable + Send + Sync + 'static>(&mut self, cacheable: CA)
        -> Result<CacheResponse, crate::error::Error<E>>;
}

#[async_trait]
impl<C, E> ReadableCache<E> for C
    where
        C: Cache<E> + Send + Sync + 'static,
        E: Debug + Clone + Send + Sync + 'static,
{

    async fn get<CA>(&mut self, cacheable: CA) -> Result<CacheResponse, crate::error::Error<E>>
        where
            CA: Cacheable + Send + Sync + 'static
    {
        Cache::get(self, cacheable).await
    }
}

#[derive(Clone)]
pub struct NopCache {}


#[async_trait]
impl<E> Cache<E> for NopCache
    where
        E: Debug + Clone + Send + Sync + 'static,
{
    async fn get<CA: Cacheable + Send + Sync + 'static,>(&mut self, _cacheable: CA)
        -> Result<CacheResponse, crate::error::Error<E>>
    {
        Ok(CacheResponse::Miss)
    }
    async fn store(&mut self, _identity: Vec<u8>) -> Result<(), crate::error::Error<E>> {
        Ok(())
    }
}
