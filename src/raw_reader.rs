use crate::ion_reader::IonReader;
use crate::raw_symbol_token::RawSymbolToken;
use std::fmt::{Display, Formatter};
use std::io::Read;

// Assuming these types are defined elsewhere in your crate.
use crate::{Blob, Clob, Decimal, Int, Timestamp};
use crate::{IonType, Str};

// Custom error type for improved error handling.
#[derive(Debug)]
pub enum IonError {
    ReadError(std::io::Error),
    ParseError(String),
    // Other error types as needed.
}

impl std::fmt::Display for IonError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            IonError::ReadError(err) => write!(f, "Read error: {}", err),
            IonError::ParseError(err) => write!(f, "Parse error: {}", err),
            // Handle other errors.
        }
    }
}

impl std::error::Error for IonError {}

// Custom result type using the IonError.
type IonResult<T> = std::result::Result<T, IonError>;

/// Represents a reader that processes raw Ion data, emitting `RawStreamItem`s and using `RawSymbolToken`.
pub trait RawReader: IonReader<Item = RawStreamItem, Symbol = RawSymbolToken> {}

impl<T> RawReader for T where T: IonReader<Item = RawStreamItem, Symbol = RawSymbolToken> {}

/// Enables using a boxed `RawReader` dynamically.
/// Note: This implementation has methods that are not object safe and cannot be invoked dynamically.
impl<R: RawReader + ?Sized> IonReader for Box<R> {
    type Item = RawStreamItem;
    type Symbol = RawSymbolToken;

    #[inline]
    fn ion_version(&self) -> (u8, u8) {
        (**self).ion_version()
    }

    // Other method implementations...
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
/// Represents the various components that a `RawReader` may encounter in an Ion stream.
pub enum RawStreamItem {
    VersionMarker(u8, u8),
    Value(IonType),
    Null(IonType),
    Nothing,
}

impl RawStreamItem {
    /// Returns an appropriate `RawStreamItem` based on the `ion_type` and nullability.
    pub fn nullable_value(ion_type: IonType, is_null: bool) -> RawStreamItem {
        if is_null {
            RawStreamItem::Null(ion_type)
        } else {
            RawStreamItem::Value(ion_type)
        }
    }
}

impl Display for RawStreamItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RawStreamItem::VersionMarker(major, minor) => {
                write!(f, "Ion version marker (v{major}.{minor})")
            }
            RawStreamItem::Value(ion_type) => write!(f, "{ion_type}"),
            RawStreamItem::Null(ion_type) => write!(f, "null.{ion_type}"),
            RawStreamItem::Nothing => write!(f, "nothing/end-of-sequence"),
        }
    }
}

/// A `RawReader` capable of non-blocking reading from a `Vec<u8>`.
pub trait BufferedRawReader: RawReader + From<Vec<u8>> {
    fn append_bytes(&mut self, bytes: &[u8]) -> IonResult<()>;
    fn read_from<R: Read>(&mut self, source: R, length: usize) -> IonResult<usize>;
    fn stream_complete(&mut self);
    fn is_stream_complete(&self) -> bool;
}

/// Trait to determine if an input type supports adding more data.
pub trait Expandable {
    fn expandable(&self) -> bool;
}

impl<T: AsRef<[u8]> + ?Sized> Expandable for &T {
    fn expandable(&self) -> bool {
        false
    }
}

impl Expandable for Vec<u8> {
    fn expandable(&self) -> bool {
        true
    }
}

// Add any other implementations or logic as needed.
