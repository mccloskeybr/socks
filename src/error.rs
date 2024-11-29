#[derive(Debug)]
pub enum Error {
    InvalidArgument(String),
    NotFound(String),
    OutOfBounds(String),
    AlreadyExists(String),
    Other(Box<dyn std::error::Error>),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>)
    -> std::fmt::Result {
        match self {
            Error::InvalidArgument(s) => write!(f, "{}", s),
            Error::NotFound(s) => write!(f, "{}", s),
            Error::OutOfBounds(s) => write!(f, "{}", s),
            Error::AlreadyExists(s) => write!(f, "{}", s),
            Error::InvalidArgument(s) => write!(f, "{}", s),
            Error::Other(e) => e.fmt(f),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self{
        Error::Other(Box::new(value))
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(value: std::array::TryFromSliceError) -> Self{
        Error::Other(Box::new(value))
    }
}

impl From<protobuf::Error> for Error {
    fn from(value: protobuf::Error) -> Self{
        Error::Other(Box::new(value))
    }
}

impl From<protobuf::text_format::ParseError> for Error {
    fn from(value: protobuf::text_format::ParseError) -> Self{
        Error::Other(Box::new(value))
    }
}
