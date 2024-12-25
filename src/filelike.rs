use crate::error::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Cursor, Read, Seek, Write};

// there isn't a unified standard interface that can be used between
// data on disk and in memory, but both are useful (for testing and
// real use).
//
// Cursor<T> can be used as an in-memory File, so create a trait to
// facilitate that behavior.

pub trait Filelike: Read + Write + Seek + Sized {
    fn create(path: &str) -> Result<Self, Error>;
}

impl Filelike for File {
    fn create(path: &str) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        Ok(file)
    }
}

impl<T: Default> Filelike for Cursor<T>
where
    Cursor<T>: Read + Write + Seek,
{
    fn create(_path: &str) -> Result<Self, Error> {
        Ok(Cursor::<T>::new(T::default()))
    }
}
