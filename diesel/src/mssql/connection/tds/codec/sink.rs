use std::{collections::VecDeque, convert::Infallible};

pub(crate) trait Sink<Item> {
    type Error;

    fn send(&mut self, item: Item) -> Result<(), Self::Error>;

    fn flush(&mut self) -> Result<(), Self::Error>;
}

impl<T> Sink<T> for Vec<T> {
    type Error = Infallible;

    fn send(&mut self, item: T) -> Result<(), Self::Error> {
        self.push(item);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<T> Sink<T> for VecDeque<T> {
    type Error = Infallible;

    fn send(&mut self, item: T) -> Result<(), Self::Error> {
        self.push_back(item);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
