use std::error::Error;

pub trait PayloadDecoder<E> {
    fn decode(&mut self, bytes: Vec<u8>) -> Result<E, Box<dyn Error>>;
}
