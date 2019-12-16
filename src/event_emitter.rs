pub trait EventEmitter {
    type Event;
    type Error: std::fmt::Debug;
    fn emit_event(&mut self, completed_events: Self::Event) -> Result<(), Self::Error>;
}
