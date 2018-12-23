extern crate aws_lambda_events;
#[macro_use]
extern crate failure;
extern crate futures;
extern crate lambda_runtime as lambda;
#[macro_use]
extern crate log;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate rusoto_sqs;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate simple_logger;
extern crate stopwatch;

use std::io::Cursor;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::thread::JoinHandle;

use aws_lambda_events::event::s3::S3Event;
use aws_lambda_events::event::sns::*;
use aws_lambda_events::event::sqs::{SqsEvent, SqsMessage};
use failure::Error;
use futures::{Future, Stream};
use lambda::Context;
use lambda::error::HandlerError;
use lambda::Handler;
use prost::Message;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, S3};
use rusoto_s3::S3Client;
use rusoto_sqs::{GetQueueUrlRequest, Sqs, SqsClient};
use serde::Deserialize;

#[derive(Debug, Fail)]
enum SqsServiceError {
    #[fail(display = "SqsMessage handler panicked with: {}", panic_msg)]
    MessageHandlerPanic {
        panic_msg: String,
    },
}

macro_rules! log_time {
    ($msg: expr, $x:expr) => {
        {
            let mut sw = stopwatch::Stopwatch::start_new();
            #[allow(path_statements)]
            let result = {$x};
            sw.stop();
            info!("{} {} milliseconds", $msg, sw.elapsed_ms());
            result
        }
    };
}

// Given a raw sqs_message, as a String, retrieve the event
pub trait EventRetriever<E> {
    fn retrieve_event(&mut self, sqs_message: String) -> Result<E, Error>;
}

#[derive(Clone)]
pub struct SnsEventRetriever<D, E, P>
    where D: EventDecoder<E> + Clone,
          P: Fn(String) -> Result<SnsEntity, Error> + Clone,
{
    parser: P,
    // Given the SnsEvent, parses out the EventType (E)
    decoder: D,
    _e: PhantomData<E>
}

impl<D, E, P> SnsEventRetriever<D, E, P>
    where D: EventDecoder<E> + Clone,
          P: Fn(String) -> Result<SnsEntity, Error> + Clone,
{
    pub fn new(
        parser: P,
        decoder: D,
    ) -> Self {
        Self {
            parser,
            decoder,
            _e: PhantomData
        }
    }
}

impl<D, E, P> EventRetriever<E> for SnsEventRetriever<D, E, P>
    where D: EventDecoder<E> + Clone,
          P: Fn(String) -> Result<SnsEntity, Error> + Clone,
{
    fn retrieve_event(&mut self, sqs_message: String) -> Result<E, Error> {
        let sns_entity = (self.parser)(sqs_message)?;
        Ok(self.decoder.decode(sns_entity.message.unwrap().into_bytes())?)
    }
}

pub struct S3EventRetriever<S, D, P, E>
    where S: S3 + Send + 'static,
          P: Fn(String) -> Result<S3Event, Error> + Clone,
          D: EventDecoder<E> + Clone,
{
    s3_client: Arc<S>,
    // Turns the SqsMessage into an S3Event. Parses out potential intermediary SNS event.
    sqs_parser: P,
    // Given the S3Event, parses out the EventType (E)
    decoder: D,
    _e: PhantomData<E>
}


impl<S, D, P, E> Clone for S3EventRetriever<S, D, P, E>
    where S: S3 + Send + 'static,
          P: Fn(String) -> Result<S3Event, Error> + Clone,
          D: EventDecoder<E> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            s3_client: self.s3_client.clone(),
            sqs_parser: self.sqs_parser.clone(),
            decoder: self.decoder.clone(),
            _e: PhantomData
        }
    }
}

impl<S, D, P, E> EventRetriever<E> for S3EventRetriever<S, D, P, E>
    where S: S3 + Send + 'static,
          P: Fn(String) -> Result<S3Event, Error> + Clone,
          D: EventDecoder<E> + Clone,
{
    fn retrieve_event(&mut self, sqs_message: String) -> Result<E, Error> {
        debug!("Parsing SqsMessage: {}", sqs_message);
        let s3_event = (self.sqs_parser)(sqs_message)?;
        let bucket = &s3_event.records[0].s3.bucket.name.as_ref().unwrap();
        let path = &s3_event.records[0].s3.object.key.as_ref().unwrap();
        let s3_object: Vec<u8> = self.read_raw_message(bucket, path)?;

        self.decoder.decode(s3_object)
    }
}



impl<S, D, P, E> S3EventRetriever<S, D, P, E>
    where S: S3 + Send + 'static,
          P: Fn(String) -> Result<S3Event, Error> + Clone,
          D: EventDecoder<E> + Clone,
{
    pub fn new(
        s3_client: Arc<S>,
        sqs_parser: P,
        decoder: D,
    ) -> Self {
        Self {
            s3_client,
            sqs_parser,
            decoder,
            _e: PhantomData,
        }
    }

    fn read_raw_message(&self, bucket: &str, path: &str) -> Result<Vec<u8>, Error>
    {
        info!("Fetching data from {} {}", bucket, path);

        let object = self.s3_client.get_object(&GetObjectRequest {
            bucket: bucket.to_owned(),
            key: path.to_owned(),
            ..GetObjectRequest::default()
        }).wait().expect(&format!("get_object {} {}", bucket, path));


        let mut body = Vec::with_capacity(5000);

        for chunk in object.body.unwrap().wait() {
            body.extend_from_slice(&chunk.unwrap());
        }

        Ok(decompressed)
    }

}

pub trait EventHandler<E>
{
    fn handle_event(&self, event: E) -> Result<(), Error>;
}


pub trait SqsCompletionHandler<S>
    where S: Sqs + Send + 'static,

{
    fn complete_message(&self, receipt_handle: String) -> Result<(), Error>;
    fn wait(&self) -> Result<(), Error>;
}

pub struct BlockingSqsCompletionHandler<S>
    where S: Sqs + Send + 'static,
{
    pub sqs_client: Arc<S>,
    pub queue_url: String,
}

impl<S> BlockingSqsCompletionHandler<S>
    where S: Sqs + Send + 'static,
{
    pub fn new(
        sqs_client: Arc<S>,
        queue_url: String,
    ) -> Self {
        Self {
            sqs_client,
            queue_url,
        }
    }
}


impl<S> Clone for BlockingSqsCompletionHandler<S>
    where S: Sqs + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            sqs_client: self.sqs_client.clone(),
            queue_url: self.queue_url.clone(),
        }
    }
}

impl<S> SqsCompletionHandler<S> for BlockingSqsCompletionHandler<S>
    where S: Sqs + Send + 'static,
{
    fn complete_message(&self, receipt_handle: String) -> Result<(), Error> {
        self.sqs_client.delete_message(
            &rusoto_sqs::DeleteMessageRequest {
                queue_url: self.queue_url.clone(),
                receipt_handle,
            }
        ).wait()?;
        Ok(())
    }

    fn wait(&self) -> Result<(), Error> {
        Ok(())
    }
}


#[derive(Clone)]
pub struct SqsService<S, R, E, H, C>
    where S: Sqs + Send + 'static,
          R: EventRetriever<E> + Clone,
          H: EventHandler<E>,
          C: SqsCompletionHandler<S> + Clone,
          E: Send + 'static
{
    pub retriever: R,
    pub handler: H,
    pub sqs_completion_handler: C,
    pub _e: PhantomData<(E, S)>
}


impl<S, R, E, H, C> SqsService<S, R, E, H, C>
    where S: Sqs + Send + 'static,
          R: EventRetriever<E> + Clone + Send + 'static,
          H: EventHandler<E> + Clone + Send + 'static,
          C: SqsCompletionHandler<S> + Clone + Send + 'static,
          E: Send + 'static
{
    pub fn new(
        retriever: R,
        handler: H,
        sqs_completion_handler: C,
    ) -> Self {
        Self {
            retriever,
            handler,
            sqs_completion_handler,
            _e: PhantomData,
        }
    }

}

impl<S, R, E, H, C> SqsService<S, R, E, H, C>
    where S: Sqs + Send + 'static,
          R: EventRetriever<E> + Clone + Send + 'static,
          H: EventHandler<E> + Clone + Send + 'static,
          C: SqsCompletionHandler<S> + Clone + Send + 'static,
          E: Send + 'static
{
    pub fn run(&mut self, event: SqsEvent, context: Context) -> Result<(), HandlerError> {
        let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::with_capacity(event.records.len());

        let mut any_err = Ok(());

        for event in event.records.into_iter() {
            let mut retriever = self.retriever.clone();
            let handler = self.handler.clone();
            let sqs_completion_handler = self.sqs_completion_handler.clone();
            let handle = std::thread::spawn(move || {
                let unparsed_event = event.body.expect("SqsMessage missing body");

                let event = retriever.retrieve_event(unparsed_event)?;

                handler.handle_event(event)?;
                sqs_completion_handler.complete_message("receipt".into())?;

                Ok(())
            });

            handles.push(handle);
        }

        for handle in handles {
            let result = handle.join();

            match result {
                Ok(Ok(_)) => (),
                Ok(e @ Err(_)) => {
                    error!("Sqs message handler failed with: {:?}", e);
                    any_err = e;
                },
                Err(e) => {
                    error!("Sqs message handler panicked with: {:?}", e);
                    any_err = Err(
                        SqsServiceError::MessageHandlerPanic {panic_msg: format!("{:?}", e)}.into()
                    );
                }
            }

        }

        if let Err(e) = self.sqs_completion_handler.wait() {
            error!("Sqs message deletion failed: {}", e);
            any_err = Err(e)
        }

        if let Err(e) = any_err {
            Err(context.new_error(&format!("{}", e)))
        } else {
            Ok(())
        }
    }
}

pub trait EventDecoder<E> {
    fn decode(&mut self, bytes: Vec<u8>) -> Result<E, Error>;
}

#[derive(Clone)]
pub struct ZstdProtoDecoder;

impl<E> EventDecoder<E> for ZstdProtoDecoder
    where E: Message + Default
{

    fn decode(&mut self, body: Vec<u8>) -> Result<E, Error>
        where E: Message + Default,
    {
        let mut decompressed = Vec::new();

        let mut body = Cursor::new(&body);

        zstd::stream::copy_decode(&mut body, &mut decompressed)?;

        Ok(E::decode(decompressed)?)
    }

}

#[derive(Clone)]
pub struct ZstdJsonDecoder {
    pub buffer: Vec<u8>
}

impl<E> EventDecoder<E> for ZstdJsonDecoder
    where E: for<'a> Deserialize<'a>
{

    fn decode(&mut self, body: Vec<u8>) -> Result<E, Error>
    {
        self.buffer.clear();
        debug!("Decompressing {} encoded bytes", body.len());
        let mut body = Cursor::new(&body);

        zstd::stream::copy_decode(&mut body, &mut self.buffer)?;
        debug!("Deserializing event from {} decompressed bytes", self.buffer.len());
        Ok(serde_json::from_slice(&self.buffer[..])?)
    }
}

pub fn events_from_s3_sns_sqs(event: String) -> Result<S3Event, Error> {
    let sns_event: SnsEntity = serde_json::from_str(&event)?;
    let event = serde_json::from_str(sns_event.message.as_ref().unwrap())?;
    Ok(event)
}

pub fn events_from_sns_sqs(event: String) -> Result<SnsEntity, Error> {
    let sns_event: SnsEntity = serde_json::from_str(&event)?;
    Ok(sns_event)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::ZstdJsonDecoder;

    use super::*;

    #[derive(Clone)]
    struct MockEventHandler;

    impl<E> EventHandler<E> for MockEventHandler {
        fn handle_event(&self, event: E) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn it_works() -> Result<(), Error> {
        let region = Region::Custom {
            name: "local".into(),
            endpoint: "http://localhost:".into()
        };

        let sqs_client = Arc::new(SqsClient::simple(region.clone()));

        let s3_client = Arc::new(S3Client::simple(region.clone()));

        let retriever: S3EventRetriever<_, _, _, ()>  = S3EventRetriever {
            s3_client,
            sqs_parser: events_from_s3_sns_sqs,
            decoder: ZstdJsonDecoder{buffer: Vec::with_capacity(1 << 8)},
            _e: PhantomData,
        };

//        let retriever: SnsEventRetriever<_, (), _> = SnsEventRetriever {
//            parser: events_from_sns_sqs,
//            decoder: ZstdJsonDecoder{buffer: Vec::with_capacity(1 << 8)},
//            _e: PhantomData
//        };

        let sqs_completion_handler = BlockingSqsCompletionHandler {
            sqs_client,
            queue_url: "".into()
        };

        let handler = MockEventHandler{};

        let mut sqs_service = SqsService {
            retriever,
            handler,
            sqs_completion_handler,
            _e: PhantomData
        };

        Ok(())
    }
}
