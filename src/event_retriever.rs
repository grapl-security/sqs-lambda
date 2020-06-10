use std::error::Error;
use std::io::Read;
use std::marker::PhantomData;
use std::time::Duration;

use tracing::info;
use rusoto_s3::{GetObjectRequest, S3};
use rusoto_sqs::Message as SqsMessage;
use tokio::prelude::*;

use async_trait::async_trait;

use crate::event_decoder::PayloadDecoder;

#[async_trait]
pub trait PayloadRetriever<T> {
    type Message;
    async fn retrieve_event(&mut self, msg: &Self::Message) -> Result<T, Box<dyn Error>>;
}

#[derive(Clone)]
pub struct S3PayloadRetriever<S, D, E>
where
    S: S3 + Clone + Send + Sync + 'static,
    D: PayloadDecoder<E> + Clone + Send + 'static,
    E: Send + 'static,
{
    s3: S,
    decoder: D,
    phantom: PhantomData<E>,
}

impl<S, D, E> S3PayloadRetriever<S, D, E>
where
    S: S3 + Clone + Send + Sync + 'static,
    D: PayloadDecoder<E> + Clone + Send + 'static,
    E: Send + 'static,
{
    pub fn new(s3: S, decoder: D) -> Self {
        Self {
            s3,
            decoder,
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<S, D, E> PayloadRetriever<E> for S3PayloadRetriever<S, D, E>
where
    S: S3 + Clone + Send + Sync + 'static,
    D: PayloadDecoder<E> + Clone + Send + 'static,
    E: Send + 'static,
{
    type Message = SqsMessage;
    #[tracing::instrument(skip(self, msg))]
    async fn retrieve_event(&mut self, msg: &Self::Message) -> Result<E, Box<dyn Error>> {
        let body = msg.body.as_ref().unwrap();
        info!("Got body from message: {}", body);
        let event: serde_json::Value = serde_json::from_str(body)?;

        let record = &event["Records"][0]["s3"];
        // let record = &event.records[0].s3;

        let bucket = record["bucket"]["name"].as_str().expect("bucket name");
        let key = record["object"]["key"].as_str().expect("object key");

        println!("{}/{}", bucket, key);

        let s3_data = self.s3.get_object(GetObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        });

        let s3_data = tokio::time::timeout(Duration::from_secs(5), s3_data).await??;
        // .with_timeout(Duration::from_secs(2)).compat().await?;

        let object_size = record["object"]["size"].as_u64().unwrap_or_default();
        let prealloc = if object_size < 1024 {
            1024
        } else {
            object_size as usize
        };

        info!("Retrieved s3 payload with size : {:?}", prealloc);

        let mut body = Vec::with_capacity(prealloc);

        s3_data
            .body
            .expect("Missing S3 body")
            .into_async_read()
            .read_to_end(&mut body)
            .await?;

        info!("Read s3 payload body");
        self.decoder.decode(body)
    }
}
