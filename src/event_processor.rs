use crate::event_handler::EventHandler;
use crate::event_retriever::EventRetriever;
use crate::sqs_consumer::SqsConsumerActor;
use crate::sqs_completion_handler::SqsCompletionHandlerActor;

use futures::sink::Sink;
use futures::stream::Stream;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use rusoto_sqs::Message as SqsMessage;
use futures::task::Context;
use std::pin::Pin;
use std::task::Poll;

use std::future::Future;

#[derive(Copy, Clone, Debug)]
pub enum ProcessorState {
    Started,
    Waiting,
    Complete,
}

#[derive(Clone)]
pub struct EventProcessor<EH, Input, Output, ER>
where
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Clone + 'static,
    ER: EventRetriever<Input> + Send + Clone + 'static,
{
    consumer: SqsConsumerActor,
    completion_handler: SqsCompletionHandlerActor<Output>,
    event_retriever: ER,
    event_handler: EH,
    state: ProcessorState,
}

impl<EH, Input, Output, ER> EventProcessor<EH, Input, Output, ER>
where
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Clone + 'static,
    ER: EventRetriever<Input> + Send + Clone + 'static,
{
    pub fn new(
        consumer: SqsConsumerActor,
        completion_handler: SqsCompletionHandlerActor<Output>,
        event_handler: EH,
        event_retriever: ER,
    ) -> Self {
        Self {
            consumer,
            completion_handler,
            event_handler,
            event_retriever,
            state: ProcessorState::Waiting,
        }
    }
}

impl<EH, Input, Output, ER> EventProcessor<EH, Input, Output, ER>
where
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Clone + 'static,
    ER: EventRetriever<Input> + Send + Clone + 'static,
{
    pub fn process_event(&mut self, event: SqsMessage) {
        // TODO: Handle errors
        let retrieved_event = match self.event_retriever.retrieve_event(&event) {
            Ok(retrieved_event) => retrieved_event,
            Err(e) => {
                return
                // TODO: Retry
                // TODO: We could reset the message visibility to 0 so it gets picked up again?
            }
        };

        let completed = match self.event_handler.handle_event(retrieved_event) {
            Ok(completed) => completed,
            Err(e) => {
                return
                // TODO: Retry
                // TODO: We could reset the message visibility to 0 so it gets picked up again?
            }
        };

        self.completion_handler.mark_complete(event, completed);

        if let ProcessorState::Started = self.state {
            self.consumer
                .get_new_event(EventProcessorActor::new(self.clone()));
        }
    }

    pub fn start_processing(&mut self) {
        self.state = ProcessorState::Started;

        self.consumer
            .get_new_event(EventProcessorActor::new(self.clone()));
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

impl<EH, Input, Output, ER> EventProcessor<EH, Input, Output, ER>
where
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Clone + 'static,
    ER: EventRetriever<Input> + Send + Clone + 'static,
{
    pub fn route_message(&mut self, msg: EventProcessorMessage) {
        match msg {
            EventProcessorMessage::process_event { event } => self.process_event(event),
            EventProcessorMessage::start_processing {} => self.start_processing(),
            EventProcessorMessage::stop_processing {} => self.stop_processing(),
        };
    }
}

#[derive(Clone)]
pub struct EventProcessorActor {
    sender: Sender<EventProcessorMessage>,
}

impl EventProcessorActor {
    pub fn new<EH, Input, Output, ER>(actor_impl: EventProcessor<EH, Input, Output, ER>) -> Self
    where
        EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Clone + 'static,
        Input: Send + Clone + 'static,
        Output: Send + Clone + 'static,
        ER: EventRetriever<Input> + Send + Clone + 'static,
    {
        let (sender, receiver) = channel(0);

        tokio::task::spawn(EventProcessorRouter {
            receiver,
            actor_impl,
        });

        Self { sender }
    }

    pub async fn process_event(&self, event: SqsMessage) {
        let msg = EventProcessorMessage::process_event { event };
        self.sender.clone().send(msg).await.map(|_| ()).map_err(|_| ());
    }

    pub async fn start_processing(&self) {
        let msg = EventProcessorMessage::start_processing {};
        self.sender.clone().send(msg).await.map(|_| ()).map_err(|_| ());
    }

    pub async fn stop_processing(&self) {
        let msg = EventProcessorMessage::stop_processing {};
        self.sender.clone().send(msg).await.map(|_| ()).map_err(|_| ());
    }
}

pub struct EventProcessorRouter<EH, Input, Output, ER>
where
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Clone + 'static,
    ER: EventRetriever<Input> + Send + Clone + 'static,
{
    receiver: Receiver<EventProcessorMessage>,
    actor_impl: EventProcessor<EH, Input, Output, ER>,
}

impl<EH, Input, Output, ER> Future for EventProcessorRouter<EH, Input, Output, ER>
where
    EH: EventHandler<InputEvent = Input, OutputEvent = Output> + Send + Clone + 'static,
    Input: Send + Clone + 'static,
    Output: Send + Clone + 'static,
    ER: EventRetriever<Input> + Send + Clone + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unimplemented!()
//        match self.receiver.poll() {
//            Ok(Poll::Ready(Some(msg))) => {
//                task::current().notify();
//                self.actor_impl.route_message(msg);
//                Ok(Poll::NotReady)
//            }
//            Ok(Poll::Ready(None)) => {
//                self.receiver.close();
//                Ok(Poll::Ready(()))
//            }
//            _ => Ok(Poll::NotReady),
//        }
    }
}
