use std::fmt::Debug;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::info;
use rusoto_s3::S3;
use rusoto_sqs::{Sqs, SendMessageRequest, SqsClient};

use crate::cache::{Cache, NopCache};
use crate::completion_event_serializer::CompletionEventSerializer;
use crate::event_decoder::PayloadDecoder;
use crate::s3_event_emitter::S3EventEmitter;
use crate::event_handler::{EventHandler, OutputEvent};
use crate::event_handler;
use crate::event_processor::{EventProcessor, EventProcessorActor};
use crate::event_retriever::S3PayloadRetriever;
use crate::sqs_completion_handler::{CompletionPolicy, SqsCompletionHandler, SqsCompletionHandlerActor};
use crate::sqs_consumer::{ConsumePolicy, SqsConsumer, SqsConsumerActor};
use aws_lambda_events::event::s3::{S3Event, S3EventRecord, S3UserIdentity, S3RequestParameters, S3Entity, S3Bucket, S3Object};
use rusoto_core::Region;
use std::future::Future;
use std::error::Error;

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

pub async fn sqs_service<
    Err,
    S3T,
    SqsT,
    EventT,
    CompletedEventT,
    EventDecoderT,
    EventEncoderT,
    EventHandlerT,
    CacheT,
    OnAck,
    EmissionResult,
    OnEmission,
>(
    queue_url: impl Into<String>,
    initial_messages: impl IntoIterator<Item=rusoto_sqs::Message>,
    dest_bucket: impl Into<String>,
    ctx: lambda_runtime::Context,
    s3_client: S3T,
    sqs_client: SqsT,
    event_decoder: EventDecoderT,
    event_encoder: EventEncoderT,
    event_handler: EventHandlerT,
    cache: CacheT,
    on_ack: OnAck,
    on_emit: OnEmission,
) -> Result<(), Box<dyn std::error::Error>>
    where
        Err: Debug + Clone + Send + Sync + 'static,
        S3T: S3 + Clone + Send + Sync + 'static,
        SqsT: Sqs + Clone + Send + Sync + 'static,
        CompletedEventT: Clone + Send + Sync + 'static,
        EventT: Clone + Send + Sync + 'static,
        EventDecoderT: PayloadDecoder<EventT> + Clone + Send + Sync + 'static,
        EventEncoderT: CompletionEventSerializer<CompletedEvent=CompletedEventT, Output=Vec<u8>, Error=<EventHandlerT as EventHandler>::Error> + Clone + Send + Sync + 'static,
        EventHandlerT: EventHandler<InputEvent=EventT, OutputEvent=CompletedEventT, Error=crate::error::Error<Err>> + Clone + Send + Sync + 'static,
        CacheT: Cache<<EventHandlerT as EventHandler>::Error> + Clone + Send + Sync + 'static,
        OnAck: Fn(SqsCompletionHandlerActor<CompletedEventT, <EventHandlerT as EventHandler>::Error, SqsT>, Result<String, String>) + Send + Sync + 'static,
        OnEmission: Fn(String, String) -> EmissionResult + Send + Sync + 'static,
        EmissionResult: Future<Output=Result<(), Box<dyn Error + Send + Sync + 'static>>>  + Send + 'static,
{
    info!("sqs service init");
    let queue_url = queue_url.into();
    let dest_bucket = dest_bucket.into();

    let consume_policy = ConsumePolicy::new(
        ctx, // Use the Context.deadline from the lambda_runtime
        Duration::from_secs(5), // Stop consuming when there's 10 seconds left in the runtime
        3, // Maximum of 3 empty receives before we stop
    );

    let (tx, shutdown_notify) = tokio::sync::oneshot::channel();

    let (sqs_completion_handler, sqs_completion_handle) = SqsCompletionHandlerActor::new(
        SqsCompletionHandler::new(
            sqs_client.clone(),
            queue_url.clone(),
            event_encoder,
            S3EventEmitter::new(
                s3_client.clone(),
                dest_bucket.clone(),
                time_based_key_fn,
                on_emit,
            ),
            CompletionPolicy::new(
                1000, // Buffer up to 1000 messages
                Duration::from_secs(30), // Buffer for up to 30 seconds
            ),
            on_ack,
            cache,
        )
    );

    let (sqs_consumer, sqs_consumer_handle) = SqsConsumerActor::new(
        SqsConsumer::new(
            sqs_client.clone(),
            queue_url.clone(),
            consume_policy,
            sqs_completion_handler.clone(),
            tx,
        )
    ).await;

    let event_processors: Vec<_> = (0..40)
        .into_iter()
        .map(|_| {
            EventProcessorActor::new(EventProcessor::new(
                sqs_consumer.clone(),
                sqs_completion_handler.clone(),
                event_handler.clone(),
                S3PayloadRetriever::new(s3_client.clone(), event_decoder.clone()),
            ))
        })
        .collect();


    futures::future::join_all(event_processors.iter().map(|ep| ep.0.start_processing())).await;

    let mut proc_iter = event_processors.iter().cycle();
    for message in initial_messages.into_iter() {
        let next_proc = proc_iter.next().unwrap();
        next_proc.0.process_event(message).await;
    }

    sqs_consumer_handle.await;
    shutdown_notify.await;
    Ok(())
}