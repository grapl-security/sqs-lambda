use std::error::Error;

pub trait EventDecoder<E> {
    fn decode(&mut self, bytes: Vec<u8>) -> Result<E, Box<dyn Error>>;
}
