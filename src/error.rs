#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub msg: String,
}

unsafe impl Send for Error {}
impl std::error::Error for Error {}

impl Error {
    pub fn new(kind: ErrorKind, msg: String) -> Self {
        Self {
            kind: kind,
            msg: msg,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    InvalidArgument,
    FailedPrecondition,
    NotFound,
    OutOfBounds,
    AlreadyExists,
    Internal,
    DataLoss,
}

impl ErrorKind {
    pub fn as_str(&self) -> &'static str {
        use ErrorKind::*;
        match *self {
            InvalidArgument => "INVALID_ARGUMENT",
            FailedPrecondition => "FAILED_PRECONDITION",
            NotFound => "NOT_FOUND",
            OutOfBounds => "OUT_OF_BOUNDS",
            AlreadyExists => "ALREADY_EXISTS",
            Internal => "INTERNAL",
            DataLoss => "DATA_LOSS",
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.msg)
    }
}
