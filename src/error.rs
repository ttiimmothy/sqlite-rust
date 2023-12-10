use nom::error::{ErrorKind, ParseError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MyError<I> {
  InvalidValueError(#[from] InvalidValueError),
  Nom(I, ErrorKind),
}

impl<I> ParseError<I> for MyError<I> {
  fn from_error_kind(input: I, kind: ErrorKind) -> Self {
    MyError::Nom(input, kind)
  }
  fn append(_: I, _: ErrorKind, other: Self) -> Self {
    other
  }
}

#[derive(Debug, Error)]
pub struct InvalidValueError(pub String);

impl std::fmt::Display for InvalidValueError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{:?}", self)
  }
}
