use rusoto_sqs::Message as SqsMessage;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use log::*;

use crate::event_handler::{EventHandler, OutputEvent, Completion};
use crate::event_retriever::PayloadRetriever;
use crate::sqs_completion_handler::CompletionHandler;
use crate::sqs_consumer::Consumer;
use std::fmt::Debug;
use crate::cache::Cache;
use std::marker::PhantomData;

#[derive(Copy, Clone, Debug)]
pub enum ProcessorState {
    Started,
    Waiting,
    Complete,
}

#[derive(Clone)]
pub struct EventProcessor<C, EH, Input, Output, ER, CH, CacheT, E>
where
    C: Consumer + Clone + Send + Sync + 'static,
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Sync + Clone + 'static,
    ER: PayloadRetriever<Input> + Send + Sync + Clone + 'static,
    CH: CompletionHandler<Message=SqsMessage, CompletedEvent=Output> + Send + Sync + Clone + 'static,
    CacheT: Cache<E> + Send + Sync + Clone + 'static,
    E: Debug + Clone + Send + Sync + 'static,
{
    consumer: C,
    completion_handler: CH,
    event_retriever: ER,
    event_handler: EH,
    state: ProcessorState,
    cache: CacheT,
    self_actor: Option<EventProcessorActor>,
    _p: PhantomData<E>
}

impl<C, EH, Input, Output, ER, CH, CacheT, E> EventProcessor<C, EH, Input, Output, ER, CH, CacheT, E>
where
    C: Consumer + Clone + Send + Sync + 'static,
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Sync + Clone + 'static,
    ER: PayloadRetriever<Input> + Send + Sync + Clone + 'static,
    CH: CompletionHandler<Message=SqsMessage, CompletedEvent=Output> + Send + Sync + Clone + 'static,
    CacheT: Cache<E> + Send + Sync + Clone + 'static,
    E: Debug + Clone + Send + Sync + 'static,
{
    pub fn new(
        consumer: C,
        completion_handler: CH,
        event_handler: EH,
        event_retriever: ER,
        cache: CacheT,
    ) -> Self {
        Self {
            consumer,
            completion_handler,
            event_handler,
            event_retriever,
            state: ProcessorState::Waiting,
            cache,
            self_actor: None,
            _p: PhantomData,
        }
    }
}

impl<C, EH, Input, Output, ER, CH, CacheT, E> EventProcessor<C, EH, Input, Output, ER, CH, CacheT, E>
where
    C: Consumer + Clone + Send + Sync + 'static,
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Sync + Clone + 'static,
    ER: PayloadRetriever<Input> + Send + Sync + Clone + 'static,
    CH: CompletionHandler<Message=SqsMessage, CompletedEvent=Output> + Send + Sync + Clone + 'static,
    CacheT: Cache<E> + Send + Sync + Clone + 'static,
    E: Debug + Clone + Send + Sync + 'static,
{
    pub async fn process_event(&mut self, event: SqsMessage) {
        // TODO: Handle errors
        info!("Retrieved event");
        let retrieved_event = match self.event_retriever.retrieve_event(&event).await {
            Ok(retrieved_event) => retrieved_event,
            Err(e) => {
                warn!("Failed to retrieve event with: {:?}", e);
                return
                // TODO: Retry
                // TODO: We could reset the message visibility to 0 so it gets picked up again?
            }
        };

        info!("Handling event");
        let output_event = self.event_handler.handle_event(retrieved_event).await;

        match output_event.completed_event {
            Completion::Total(completed) => {
                info!("Marking all events complete - total success");
                self.completion_handler.mark_complete(event, completed).await;
            },
            Completion::Partial((_completed, err)) => {
                warn!("EventHandler was only partially successful: {:?}", err);
            },
            Completion::Error(e) => {
                info!("Event handler failed: {:?}", e);
                return
                // TODO: Retry
                // TODO: We could reset the message visibility to 0 so it gets picked up again?
            }
        };

        info!("Caching");

        for identity in output_event.identities {
            if let Err(e) = self.cache.store(identity).await {
                warn!("Failed to cache with: {:?}", e);
            }
        }

        if let ProcessorState::Started = self.state {
            self.consumer
                .get_next_event(self.self_actor.clone().unwrap()).await;
        }
    }

    pub async fn start_processing(&mut self) {
        self.state = ProcessorState::Started;

        info!("Getting next event from consumer");
        self.consumer
            .get_next_event(self.self_actor.clone().unwrap()).await;
    }

    pub fn stop_processing(&mut self) {
        self.state = ProcessorState::Complete;
    }
}

#[allow(non_camel_case_types)]
pub enum EventProcessorMessage {
    process_event { event: SqsMessage },
    start_processing {},
    stop_processing {},
}

impl<C, EH, Input, Output, ER, CH, CacheT, E> EventProcessor<C, EH, Input, Output, ER, CH, CacheT, E>
where
    C: Consumer + Clone + Send + Sync + 'static,
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Sync + Clone + 'static,
    ER: PayloadRetriever<Input> + Send + Sync + Clone + 'static,
    CH: CompletionHandler<Message=SqsMessage, CompletedEvent=Output> + Send + Sync + Clone + 'static,
    CacheT: Cache<E> + Send + Sync + Clone + 'static,
    E: Debug + Clone + Send + Sync + 'static,
{
    pub async fn route_message(&mut self, msg: EventProcessorMessage) {
        match msg {
            EventProcessorMessage::process_event { event } => self.process_event(event).await,
            EventProcessorMessage::start_processing {} => self.start_processing().await,
            EventProcessorMessage::stop_processing {} => self.stop_processing(),
        };
    }
}

#[derive(Clone)]
pub struct EventProcessorActor {
    sender: Sender<EventProcessorMessage>,
}

impl EventProcessorActor {
    pub fn new<C, EH, Input, Output, ER, CH, CacheT, E>(mut actor_impl: EventProcessor<C, EH, Input, Output, ER, CH, CacheT, E>) -> Self
    where
        C: Consumer + Clone + Send + Sync + 'static,
        EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
        Input: Send + Clone + 'static,
        Output: Send + Sync + Clone + 'static,
        ER: PayloadRetriever<Input> + Send + Sync + Clone + 'static,
        CH: CompletionHandler<Message=SqsMessage, CompletedEvent=Output> + Send + Sync + Clone + 'static,
        CacheT: Cache<E> + Send + Sync + Clone + 'static,
        E: Debug + Clone + Send + Sync + 'static,
    {
        let (sender, receiver) = channel(1);

        let self_actor = Self {sender};
        actor_impl.self_actor = Some(self_actor.clone());

        tokio::task::spawn(
            route_wrapper(
                EventProcessorRouter {
                    receiver,
                    actor_impl,
                }
            )
        );

        self_actor
    }

    pub async fn process_event(&self, event: SqsMessage) {
        let msg = EventProcessorMessage::process_event { event };
        if let Err(_e) = self.sender.clone().send(msg).await {
            panic!("Receiver has failed, propagating error. process_event")
        };
    }

    pub async fn start_processing(&self) {
        let msg = EventProcessorMessage::start_processing {};
        if let Err(_e) = self.sender.clone().send(msg).await {
            panic!("Receiver has failed, propagating error. start_processing")
        };
    }

    pub async fn stop_processing(&self) {
        let msg = EventProcessorMessage::stop_processing {};
        if let Err(_e) = self.sender.clone().send(msg).await {
            panic!("Receiver has failed, propagating error. stop_processing")
        };
    }
}

pub struct EventProcessorRouter<C, EH, Input, Output, ER, CH, CacheT, E>
where
    C: Consumer + Clone + Send + Sync + 'static,
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Sync + Clone + 'static,
    ER: PayloadRetriever<Input> + Send + Sync + Clone + 'static,
    CH: CompletionHandler<Message=SqsMessage, CompletedEvent=Output> + Send + Sync + Clone + 'static,
    CacheT: Cache<E> + Send + Sync + Clone + 'static,
    E: Debug + Clone + Send + Sync + 'static,
{
    receiver: Receiver<EventProcessorMessage>,
    actor_impl: EventProcessor<C, EH, Input, Output, ER, CH, CacheT, E>,
}


async fn route_wrapper<C, EH, Input, Output, ER, CH, CacheT, E>(mut router: EventProcessorRouter<C, EH, Input, Output, ER, CH, CacheT, E>)
where
    C: Consumer + Clone + Send + Sync + 'static,
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Sync + Clone + 'static,
    ER: PayloadRetriever<Input> + Send + Sync + Clone + 'static,
    CH: CompletionHandler<Message=SqsMessage, CompletedEvent=Output> + Send + Sync + Clone + 'static,
    CacheT: Cache<E> + Send + Sync + Clone + 'static,
    E: Debug + Clone + Send + Sync + 'static,
{
    while let Some(msg) = router.receiver.recv().await {
        router.actor_impl.route_message(msg).await;
    }
}
