use log::*;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::completion_handler::CompletionHandler;
use crate::consumer::Consumer;
use crate::event_handler::{Completion, EventHandler, OutputEvent};
use crate::event_retriever::PayloadRetriever;
use std::fmt::Debug;

use aktors::actor::Actor;
use async_trait::async_trait;
use std::marker::PhantomData;

#[derive(Copy, Clone, Debug)]
pub enum ProcessorState {
    Started,
    Waiting,
    Complete,
}

#[derive(Clone)]
pub struct EventProcessor<M, C, EH, Input, Output, ER, CH>
where
    M: Send + Clone + Sync + 'static,
    C: Consumer<M> + Clone + Send + Sync + 'static,
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Sync + Clone + 'static,
    ER: PayloadRetriever<Input, Message = M> + Send + Sync + Clone + 'static,
    CH: CompletionHandler<
            Message = M,
            CompletedEvent = OutputEvent<Output, <EH as EventHandler>::Error>,
        > + Send
        + Sync
        + Clone
        + 'static,
{
    consumer: C,
    completion_handler: CH,
    event_retriever: ER,
    event_handler: EH,
    state: ProcessorState,
    self_actor: Option<EventProcessorActor<M>>,
}

impl<M, C, EH, Input, Output, ER, CH> EventProcessor<M, C, EH, Input, Output, ER, CH>
where
    M: Send + Clone + Sync + 'static,
    C: Consumer<M> + Clone + Send + Sync + 'static,
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Sync + Clone + 'static,
    ER: PayloadRetriever<Input, Message = M> + Send + Sync + Clone + 'static,
    CH: CompletionHandler<
            Message = M,
            CompletedEvent = OutputEvent<Output, <EH as EventHandler>::Error>,
        > + Send
        + Sync
        + Clone
        + 'static,
{
    pub fn new(
        consumer: C,
        completion_handler: CH,
        event_handler: EH,
        event_retriever: ER,
    ) -> Self {
        Self {
            consumer,
            completion_handler,
            event_handler,
            event_retriever,
            state: ProcessorState::Waiting,
            self_actor: None,
        }
    }
}

impl<M, C, EH, Input, Output, ER, CH> EventProcessor<M, C, EH, Input, Output, ER, CH>
where
    M: Send + Clone + Sync + 'static,
    C: Consumer<M> + Clone + Send + Sync + 'static,
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Sync + Clone + 'static,
    ER: PayloadRetriever<Input, Message = M> + Send + Sync + Clone + 'static,
    CH: CompletionHandler<
            Message = M,
            CompletedEvent = OutputEvent<Output, <EH as EventHandler>::Error>,
        > + Send
        + Sync
        + Clone
        + 'static,
{
    pub async fn process_event(&mut self, event: M) {
        // TODO: Handle errors
        info!("Processing event");
        let retrieved_event = match self.event_retriever.retrieve_event(&event).await {
            Ok(retrieved_event) => {
                info!("Retrieved event");
                Some(retrieved_event)
            }
            Err(e) => {
                warn!("Failed to retrieve event with: {:?}", e);
                None
                // TODO: Retry
                // TODO: We could reset the message visibility to 0 so it gets picked up again?
            }
        };

        if let Some(retrieved_event) = retrieved_event {
            info!("Handling retrieved event");
            let output_event = self.event_handler.handle_event(retrieved_event).await;
            self.completion_handler
                .mark_complete(event, output_event)
                .await;
        }

        info!("self.processor_state {:?}", self.state);
        if let ProcessorState::Started = self.state {
            info!("Getting next event");
            self.consumer
                .get_next_event(
                    self.self_actor
                        .clone()
                        .expect("event_processor, self_actor"),
                )
                .await;
        }
    }

    pub async fn start_processing(&mut self) {
        self.state = ProcessorState::Started;

        info!("Getting next event from consumer");
        self.consumer
            .get_next_event(self.self_actor.clone().unwrap())
            .await;
    }

    pub fn stop_processing(&mut self) {
        info!("stop_processing");
        self.state = ProcessorState::Complete;
    }
}

#[allow(non_camel_case_types)]
pub enum EventProcessorMessage<M>
where
    M: Send + Clone + Sync + 'static,
{
    process_event { event: M },
    start_processing {},
    stop_processing {},
    release,
}

impl<M> aktors::actor::Message for EventProcessorMessage<M>
where
    M: Send + Clone + Sync + 'static,
{
    fn is_release(&self) -> bool {
        if let Self::release = self {
            true
        } else {
            false
        }
    }
}

#[async_trait]
impl<M, C, EH, Input, Output, ER, CH> Actor<EventProcessorMessage<M>>
    for EventProcessor<M, C, EH, Input, Output, ER, CH>
where
    M: Send + Clone + Sync + 'static,
    C: Consumer<M> + Clone + Send + Sync + 'static,
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Sync + Clone + 'static,
    ER: PayloadRetriever<Input, Message = M> + Send + Sync + Clone + 'static,
    CH: CompletionHandler<
            Message = M,
            CompletedEvent = OutputEvent<Output, <EH as EventHandler>::Error>,
        > + Send
        + Sync
        + Clone
        + 'static,
{
    async fn route_message(&mut self, msg: EventProcessorMessage<M>) {
        match msg {
            EventProcessorMessage::process_event { event } => self.process_event(event).await,
            EventProcessorMessage::start_processing {} => self.start_processing().await,
            EventProcessorMessage::stop_processing {} => self.stop_processing(),
            EventProcessorMessage::release => (),
        };
    }

    fn get_actor_name(&self) -> &str {
        &self.self_actor.as_ref().unwrap().actor_name
    }

    fn close(&mut self) {
        self.self_actor = None;
    }
}

pub struct EventProcessorActor<M>
where
    M: Send + Clone + Sync + 'static,
{
    sender: Sender<EventProcessorMessage<M>>,
    inner_rc: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    queue_len: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    actor_name: String,
    actor_uuid: uuid::Uuid,
    actor_num: usize,
}

impl<M> Clone for EventProcessorActor<M>
where
    M: Send + Clone + Sync + 'static,
{
    fn clone(&self) -> Self {
        self.inner_rc
            .clone()
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Self {
            sender: self.sender.clone(),
            inner_rc: self.inner_rc.clone(),
            queue_len: self.queue_len.clone(),
            actor_name: format!(
                "{} {} {}",
                stringify!(EventProcessorActor),
                self.actor_uuid,
                self.actor_num + 1,
            ),
            actor_uuid: self.actor_uuid,
            actor_num: self.actor_num + 1,
        }
    }
}

impl<M> EventProcessorActor<M>
where
    M: Send + Clone + Sync + 'static,
{
    pub fn new<C, EH, Input, Output, ER, CH>(
        mut actor_impl: EventProcessor<M, C, EH, Input, Output, ER, CH>,
    ) -> (Self, tokio::task::JoinHandle<()>)
    where
        C: Consumer<M> + Clone + Send + Sync + 'static,
        EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Sync + Clone + 'static,
        Input: Send + Clone + 'static,
        Output: Send + Sync + Clone + 'static,
        ER: PayloadRetriever<Input, Message = M> + Send + Sync + Clone + 'static,
        CH: CompletionHandler<
                Message = M,
                CompletedEvent = OutputEvent<Output, <EH as EventHandler>::Error>,
            > + Send
            + Sync
            + Clone
            + 'static,
    {
        let (sender, receiver) = channel(1);
        let inner_rc = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(1));
        let queue_len = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let actor_uuid = uuid::Uuid::new_v4();
        let actor_name = format!("{} {} {}", stringify!(EventProcessor), actor_uuid, 0,);
        let inner_actor = Self {
            sender,
            inner_rc: inner_rc.clone(),
            queue_len: queue_len.clone(),
            actor_name,
            actor_uuid,
            actor_num: 0,
        };

        let self_actor = inner_actor.clone();

        actor_impl.self_actor = Some(inner_actor);

        let handle = tokio::task::spawn(aktors::actor::route_wrapper(aktors::actor::Router::new(
            actor_impl, receiver, inner_rc, queue_len,
        )));

        (self_actor, handle)
    }

    pub async fn release(self) {
        let msg = EventProcessorMessage::release;

        let mut sender = self.sender.clone();

        let queue_len = self.queue_len.clone();
        queue_len.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        tokio::task::spawn(async move {
            sender.send(msg).await;
        });
    }

    pub async fn process_event(&self, event: M) {
        let msg = EventProcessorMessage::process_event { event };

        let mut sender = self.sender.clone();

        let queue_len = self.queue_len.clone();
        queue_len.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        tokio::task::spawn(async move {
            if let Err(e) = sender.send(msg).await {
                panic!(
                    concat!(
                        "Receiver has failed with {}, propagating error. ",
                        "EventProcessorActor.process_event"
                    ),
                    e
                )
            }
        });
    }

    pub async fn start_processing(&self) {
        let msg = EventProcessorMessage::start_processing {};

        let mut sender = self.sender.clone();

        let queue_len = self.queue_len.clone();
        queue_len.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        tokio::task::spawn(async move {
            if let Err(e) = sender.send(msg).await {
                panic!(
                    concat!(
                        "Receiver has failed with {}, propagating error. ",
                        "EventProcessorActor.start_processing"
                    ),
                    e
                )
            }
        });
    }

    pub async fn stop_processing(&self) {
        let msg = EventProcessorMessage::stop_processing {};

        let mut sender = self.sender.clone();

        let queue_len = self.queue_len.clone();

        queue_len.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tokio::task::spawn(async move {
            if let Err(e) = sender.send(msg).await {
                panic!(
                    concat!(
                        "Receiver has failed with {}, propagating error. ",
                        "EventProcessorActor.stop_processing"
                    ),
                    e
                )
            }
        });
    }
}

impl<M> Drop for EventProcessorActor<M>
where
    M: Send + Clone + Sync + 'static,
{
    fn drop(&mut self) {
        self.inner_rc
            .clone()
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
