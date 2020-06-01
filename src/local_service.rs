use log::info;

use crate::completion_event_serializer::CompletionEventSerializer;
use crate::event_decoder::PayloadDecoder;
use crate::event_handler;
use crate::event_handler::{EventHandler, OutputEvent};
use crate::event_processor::{EventProcessor, EventProcessorActor};
use crate::fs_completion_handler::{FsCompletionHandler, FsCompletionHandlerActor};
use crate::fs_event_emitter::FsEventEmitter;
use crate::fs_event_retriever::FsRetriever;
use crate::fs_notify_consumer::{FsNotifyConsumerHandler, FsNotifyConsumerHandlerActor};
use std::fmt::Debug;

pub async fn local_service<
    Err,
    EventEncoderT,
    CompletedEventT,
    EventDecoderT,
    EventT,
    EventHandlerT,
>(
    input_directory: impl AsRef<str>,
    output_directory: impl AsRef<str>,
    event_encoder: EventEncoderT,
    event_decoder: EventDecoderT,
    event_handler: EventHandlerT,
) -> Result<(), Box<dyn std::error::Error>>
where
    Err: Debug + Clone + Send + Sync + 'static,
    CompletedEventT: Clone + Send + Sync + 'static,
    EventT: Clone + Send + Sync + 'static,
    EventEncoderT: CompletionEventSerializer<
            CompletedEvent = CompletedEventT,
            Output = Vec<u8>,
            Error = crate::error::Error<Err>,
        > + Clone
        + Send
        + Sync
        + 'static,
    EventDecoderT: PayloadDecoder<EventT> + Clone + Send + Sync + 'static,
    EventHandlerT: EventHandler<
            InputEvent = EventT,
            OutputEvent = CompletedEventT,
            Error = crate::error::Error<Err>,
        > + Clone
        + Send
        + Sync
        + 'static,
{
    let input_directory = input_directory.as_ref();
    let output_directory = output_directory.as_ref();

    let fs_event_emitter = FsEventEmitter::new(output_directory);

    let (fs_completion_handler, fs_completion_handle) =
        FsCompletionHandlerActor::new(FsCompletionHandler::new(event_encoder, fs_event_emitter))
            .await;

    let (fs_notify_consumer, fs_notify_handle) = FsNotifyConsumerHandlerActor::new(
        FsNotifyConsumerHandler::new(input_directory, fs_completion_handler.clone()),
    )
    .await;

    let event_processors: Vec<_> = (0..40)
        .into_iter()
        .map(|_| {
            EventProcessorActor::new(EventProcessor::new(
                fs_notify_consumer.clone(),
                fs_completion_handler.clone(),
                event_handler.clone(),
                FsRetriever::new(event_decoder.clone()),
            ))
        })
        .collect();

    futures::future::join_all(event_processors.iter().map(|ep| ep.0.start_processing())).await;
    info!("started processors");

    // futures::future::join_all(event_processors.into_iter().map(|ep| ep.0.release())).await;
    futures::future::join_all(event_processors.into_iter().map(|ep| ep.1)).await;

    fs_notify_handle.await;
    fs_completion_handle.await;
    // fs_notify_handle.await;
    // info!("fs_notify_handle");
    // info!("fs_completion_handle");
    // info!("event_processors.await");

    info!("Complete");
    Ok(())
}
