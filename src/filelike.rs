use crate::error::{Error, ErrorKind::*};
use std::fmt::Debug;
use std::io::Cursor;
use std::marker::Unpin;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

// there isn't a unified standard interface that can be used between
// data on disk and in memory, but both are useful (for testing and
// real use).
//
// Cursor<T> can be used as an in-memory File, so create a trait to
// facilitate that behavior.

#[allow(async_fn_in_trait)]
pub trait Filelike: Debug + Unpin + Send + AsyncRead + AsyncWrite + AsyncSeek + Sized {
    async fn create(path: &str) -> Result<Self, Error>;
}

impl Filelike for File {
    async fn create(path: &str) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .await
            .map_err(|e| Error::new(FailedPrecondition, format!("Unable to open file: {e}")))?;
        Ok(file)
    }
}

impl<T: Default> Filelike for Cursor<T>
where
    Cursor<T>: Debug + Unpin + Send + AsyncRead + AsyncWrite + AsyncSeek,
{
    async fn create(_path: &str) -> Result<Self, Error> {
        Ok(Cursor::<T>::new(T::default()))
    }
}
