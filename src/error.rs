#[derive(Debug)]
pub enum Error {
    InvalidArgument(String),
    FailedPrecondition(String),
    NotFound(String),
    OutOfBounds(String),
    AlreadyExists(String),
    Internal(String),
    DataLoss(String),
}

unsafe impl Send for Error {}
impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidArgument(s) => write!(f, "{}", s),
            Error::FailedPrecondition(s) => write!(f, "{}", s),
            Error::NotFound(s) => write!(f, "{}", s),
            Error::OutOfBounds(s) => write!(f, "{}", s),
            Error::AlreadyExists(s) => write!(f, "{}", s),
            Error::Internal(s) => write!(f, "{}", s),
            Error::DataLoss(s) => write!(f, "{}", s),
        }
    }
}
