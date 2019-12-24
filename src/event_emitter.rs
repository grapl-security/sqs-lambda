use std::error::Error;
use std::time::Duration;

use futures::compat::Future01CompatExt;
use log::*;
use rusoto_s3::{PutObjectRequest, S3};

use async_trait::async_trait;

#[async_trait]
pub trait EventEmitter {
    type Event;
    type Error: std::fmt::Debug;
    async fn emit_event(&mut self, completed_events: Vec<Self::Event>) -> Result<(), Self::Error>;
}

#[derive(Clone)]
pub struct S3EventEmitter<S, F>
    where
        S: S3 + Send + 'static,
        F: Fn(&[u8]) -> String,
{
    s3: S,
    output_bucket: String,
    key_fn: F,
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
            s3,
            output_bucket,
            key_fn,
        }
    }
}

#[async_trait]
impl<S, F> EventEmitter for S3EventEmitter<S, F>
    where
        S: S3 + Send + Sync + 'static,
        F: Fn(&[u8]) -> String + Send + Sync,
{
    type Event = Vec<u8>;
    type Error = Box<dyn Error>;

    async fn emit_event(&mut self, events: Vec<Self::Event>) -> Result<(), Self::Error> {
        let event_uploads = events.into_iter().map(|event| {
            let key = (self.key_fn)(&event);

            info!("Uploading: {} bytes to {}{}", event.len(), &self.output_bucket, &key);

            self.s3.put_object(
                PutObjectRequest {
                    body: Some(event.into()),
                    bucket: self.output_bucket.clone(),
                    key,
                    ..Default::default()
                }
            )
                .with_timeout(Duration::from_secs(2))
                .compat()
        });

        let event_uploads = futures::future::join_all(event_uploads).await;

        for upload in event_uploads {
            upload?;
        }

        Ok(())
    }
}