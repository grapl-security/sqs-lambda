extern crate futures;
extern crate rusoto_s3;
extern crate rusoto_sqs;
extern crate sqs_lambda;
extern crate tokio;

use std::collections::HashMap;
use std::error::Error;
use std::io::Cursor;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;
use rusoto_s3::S3Client;
use rusoto_sqs::SqsClient;
use serde::Deserialize;

use async_trait::async_trait;
use event_processor::*;
use sqs_completion_handler::*;
use sqs_consumer::*;
use sqs_lambda::completion_event_serializer::CompletionEventSerializer;
use sqs_lambda::event_decoder::EventDecoder;
use sqs_lambda::event_emitter::{S3EventEmitter};
use sqs_lambda::event_handler::EventHandler;
use sqs_lambda::event_processor;
use sqs_lambda::event_retriever::S3EventRetriever;
use sqs_lambda::sqs_completion_handler;
use sqs_lambda::sqs_consumer;

#[derive(Debug, Clone)]
struct MyService {}

#[async_trait]
impl EventHandler for MyService {
    type InputEvent = HashMap<String, String>;
    type OutputEvent = Subgraph;
    type Error = ();

    async fn handle_event(&mut self, _input: Self::InputEvent) -> Result<Self::OutputEvent, Self::Error> {
        // do some work

        Ok(Subgraph {})
    }
}


#[derive(Clone, Debug)]
pub struct Subgraph {}

impl Subgraph {
    fn merge(&mut self, _other: &Self) {
        unimplemented!()
    }

    fn into_bytes(self) -> Vec<u8> {
        unimplemented!()
    }
}

#[derive(Clone, Debug)]
pub struct SubgraphSerializer {}

impl CompletionEventSerializer for SubgraphSerializer {
    type CompletedEvent = Subgraph;
    type Output = Vec<u8>;
    type Error = ();

    fn serialize_completed_events(
        &mut self,
        completed_events: &[Self::CompletedEvent],
    ) -> Result<Self::Output, Self::Error> {
        let mut subgraph = Subgraph {};
        for sg in completed_events {
            subgraph.merge(sg);
        }

//        subgraph.into_bytes()
        Ok(vec![])
    }
}

#[derive(Clone)]
pub struct ZstdProtoDecoder;

impl<E> EventDecoder<E> for ZstdProtoDecoder
    where E: Message + Default
{

    fn decode(&mut self, body: Vec<u8>) -> Result<E, Box<dyn Error>>
        where E: Message + Default,
    {
        let mut decompressed = Vec::new();

        let mut body = Cursor::new(&body);

        zstd::stream::copy_decode(&mut body, &mut decompressed)?;

        Ok(E::decode(decompressed)?)
    }

}

#[derive(Clone, Default)]
pub struct ZstdDecoder {
    pub buffer: Vec<u8>
}

impl EventDecoder<Vec<u8>> for ZstdDecoder
{

    fn decode(&mut self, body: Vec<u8>) -> Result<Vec<u8>, Box<dyn Error>>
    {
        self.buffer.clear();

        let mut body = Cursor::new(&body);

        zstd::stream::copy_decode(&mut body, &mut self.buffer)?;

        Ok(self.buffer.clone())
    }
}

#[derive(Clone, Default)]
pub struct ZstdJsonDecoder {
    pub buffer: Vec<u8>
}

impl<E> EventDecoder<E> for ZstdJsonDecoder
    where E: for<'a> Deserialize<'a>
{

    fn decode(&mut self, body: Vec<u8>) -> Result<E, Box<dyn Error>>
    {
        self.buffer.clear();

        let mut body = Cursor::new(&body);

        zstd::stream::copy_decode(&mut body, &mut self.buffer)?;

        Ok(serde_json::from_slice(&self.buffer[..])?)
    }
}


fn init_sqs_client() -> SqsClient
{
    unimplemented!()
}

fn init_s3_client() -> S3Client
{
    unimplemented!()
}

fn time_based_key_fn(_event: &[u8]) -> String {
    let cur_ms = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_millis(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    };

    let cur_day = cur_ms - (cur_ms % 86400);

    format!(
        "{}/{}-{}",
        cur_day, cur_ms, uuid::Uuid::new_v4()
    )
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::spawn(async move {
        let consume_policy = ConsumePolicy::new(
            unimplemented!(), // Use the Context.deadline from the lambda_runtime
            Duration::from_secs(5), // Stop consuming when there's 10 seconds left in the runtime
        );

        let (tx, shutdown_notify) = tokio::sync::oneshot::channel();

        let sqs_consumer = SqsConsumerActor::new(
            SqsConsumer::new(init_sqs_client(), "queue_url".into(), consume_policy, tx)
        );

        let sqs_completion_handler = SqsCompletionHandlerActor::new(
            SqsCompletionHandler::new(
                init_sqs_client(),
                "queue_url".to_string(),
                SubgraphSerializer {},
                S3EventEmitter::new(
                    init_s3_client(),
                    "SomeBucket".to_owned(),
                    time_based_key_fn,
                ),
                CompletionPolicy::new(
                    1000, // Buffer up to 1000 messages
                    Duration::from_secs(30), // Buffer for up to 30 seconds
                )
            )
        );

        let event_processors: Vec<_> = (0..40)
            .into_iter()
            .map(|_| {
                EventProcessorActor::new(EventProcessor::new(
                    sqs_consumer.clone(),
                    sqs_completion_handler.clone(),
                    MyService {},
                    S3EventRetriever::new(init_s3_client(), ZstdJsonDecoder::default()),
                ))
            })
            .collect();

        futures::future::join_all(event_processors.iter().map(|ep| ep.start_processing())).await;

        // Wait for the consumers to shutdown
        let _ = shutdown_notify.await;

        tokio::time::delay_for(Duration::from_millis(100)).await;

    });

    Ok(())
}

