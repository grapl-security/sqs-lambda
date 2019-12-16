use std::fmt::Debug;

pub trait EventHandler {
    type InputEvent;
    type OutputEvent;
    type Error: Debug;

    fn handle_event(&mut self, input: Self::InputEvent) -> Result<Self::OutputEvent, Self::Error>;
}
