use std::fmt::Debug;
use async_trait::async_trait;

#[async_trait]
pub trait EventHandler {
    type InputEvent;
    type OutputEvent;
    type Error: Debug;

    async fn handle_event(&mut self, input: Self::InputEvent) -> Result<Self::OutputEvent, Self::Error>;
}
