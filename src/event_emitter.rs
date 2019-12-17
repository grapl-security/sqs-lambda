use async_trait::async_trait;
use rusoto_s3::{S3, PutObjectRequest};
use futures::compat::Future01CompatExt;
use std::error::Error;

#[async_trait]
pub trait EventEmitter {
    type Event;
    type Error: std::fmt::Debug;
    async fn emit_event(&mut self, completed_events: Self::Event) -> Result<(), Self::Error>;
}

#[derive(Clone)]
pub struct S3EventEmitter<S, F>
    where
        S: S3 + Send + 'static,
        F: Fn(&[u8]) -> String,
{
    s3: S,
    output_bucket: String,
    key_fn: F
}

impl<S, F> S3EventEmitter<S, F>
    where
        S: S3 + Send + 'static,
        F: Fn(&[u8]) -> String,
{
    pub fn new(s3: S, output_bucket: impl Into<String>, key_fn: F) -> Self
    {
        let output_bucket = output_bucket.into();
        Self {
            s3, output_bucket, key_fn
        }
    }
}

#[async_trait]
impl<S, F> EventEmitter for S3EventEmitter<S, F>
    where
        S: S3 + Send + Sync + 'static,
        F: Fn(&[u8]) -> String + Send,
{
    type Event = Vec<u8>;
    type Error = Box<dyn Error>;

    async fn emit_event(&mut self, event: Self::Event) -> Result<(), Self::Error> {
        let key = (self.key_fn)(&event);

        self.s3.put_object(
            PutObjectRequest {
                body: Some(event.into()),
                bucket: "".to_string(),
                key,
                ..Default::default()
            }
        ).compat().await?;

        Ok(())
    }
}