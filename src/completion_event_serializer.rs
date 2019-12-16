pub trait CompletionEventSerializer {
    type CompletedEvent;
    type Output;
    fn serialize_completed_events(
        &mut self,
        completed_events: &[Self::CompletedEvent],
    ) -> Self::Output;
}
