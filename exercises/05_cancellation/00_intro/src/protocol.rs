//! The wire protocol: one request per line, one response per line.

use std::fmt::{self, Display, Formatter};

use crate::{Bucket, Key, NameError, Value, ValueError};

/// A request from a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Get {
        bucket: Bucket,
        key: Key,
    },
    Set {
        bucket: Bucket,
        key: Key,
        value: Value,
    },
    Del {
        bucket: Bucket,
        key: Key,
    },
}

impl Request {
    /// Parses a request line, which is what a server does with everything a client sends.
    pub fn parse(line: &str) -> Result<Self, ProtocolError> {
        let mut parts = line.splitn(4, ' ');

        let verb = parts.next().unwrap_or_default().to_ascii_uppercase();
        if verb.is_empty() {
            return Err(ProtocolError::Empty);
        }
        if !matches!(verb.as_str(), "GET" | "SET" | "DEL") {
            return Err(ProtocolError::UnknownVerb(verb));
        }

        let bucket = Bucket::parse(parts.next().ok_or(ProtocolError::MissingArgument)?)?;
        let key = Key::parse(parts.next().ok_or(ProtocolError::MissingArgument)?)?;

        match verb.as_str() {
            "GET" => Ok(Self::Get { bucket, key }),
            "DEL" => Ok(Self::Del { bucket, key }),
            _ => {
                let value = Value::parse(parts.next().ok_or(ProtocolError::MissingArgument)?)?;
                Ok(Self::Set { bucket, key, value })
            }
        }
    }
}

impl Display for Request {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Get { bucket, key } => write!(f, "GET {} {}", bucket.as_str(), key.as_str()),
            Self::Del { bucket, key } => write!(f, "DEL {} {}", bucket.as_str(), key.as_str()),
            Self::Set { bucket, key, value } => write!(
                f,
                "SET {} {} {}",
                bucket.as_str(),
                key.as_str(),
                value.as_str()
            ),
        }
    }
}

/// A reply to a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Ok,
    Value(Value),
    Nil,
    Error(String),
}

impl Response {
    /// Parses a response line, which is what a client does with everything a server sends.
    pub fn parse(line: &str) -> Result<Self, ProtocolError> {
        let (verb, rest) = match line.split_once(' ') {
            Some((verb, rest)) => (verb, Some(rest)),
            None => (line, None),
        };

        match (verb, rest) {
            ("OK", None) => Ok(Self::Ok),
            ("NIL", None) => Ok(Self::Nil),
            ("VALUE", Some(rest)) => Ok(Self::Value(Value::parse(rest)?)),
            ("ERR", Some(rest)) => Ok(Self::Error(rest.to_owned())),
            ("", None) => Err(ProtocolError::Empty),
            _ => Err(ProtocolError::UnknownVerb(verb.to_owned())),
        }
    }
}

impl Display for Response {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => write!(f, "OK"),
            Self::Nil => write!(f, "NIL"),
            Self::Value(value) => write!(f, "VALUE {}", value.as_str()),
            Self::Error(message) => write!(f, "ERR {message}"),
        }
    }
}

/// What can go wrong reading a line off the wire.
#[derive(Debug, PartialEq, Eq)]
pub enum ProtocolError {
    Empty,
    UnknownVerb(String),
    MissingArgument,
    BadName(NameError),
    BadValue(ValueError),
}

impl Display for ProtocolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "empty request"),
            Self::UnknownVerb(verb) => write!(f, "unknown verb {verb}"),
            Self::MissingArgument => write!(f, "missing argument"),
            Self::BadName(error) => write!(f, "invalid name: {error:?}"),
            Self::BadValue(error) => write!(f, "invalid value: {error:?}"),
        }
    }
}

impl From<NameError> for ProtocolError {
    fn from(error: NameError) -> Self {
        Self::BadName(error)
    }
}

impl From<ValueError> for ProtocolError {
    fn from(error: ValueError) -> Self {
        Self::BadValue(error)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Value, ValueError,
        protocol::{ProtocolError, Request, Response},
    };

    #[test]
    fn every_request_survives_the_round_trip() {
        for line in [
            "GET users alice",
            "DEL users alice",
            "SET users alice hello",
            "SET users alice a value with spaces in it",
        ] {
            let request = Request::parse(line).expect("a valid request");
            assert_eq!(request.to_string(), line);
        }
    }

    #[test]
    fn every_response_survives_it_too() {
        for line in ["OK", "NIL", "VALUE hello", "ERR unknown bucket"] {
            let response = Response::parse(line).expect("a valid response");
            assert_eq!(response.to_string(), line);
        }
    }

    #[test]
    fn a_verb_nobody_implements_is_reported_as_such() {
        assert_eq!(
            Request::parse("PING"),
            Err(ProtocolError::UnknownVerb("PING".to_owned()))
        );
    }

    #[test]
    fn a_request_missing_its_key_is_not_a_request() {
        assert_eq!(
            Request::parse("GET users"),
            Err(ProtocolError::MissingArgument)
        );
    }

    #[test]
    fn a_value_with_a_newline_cannot_be_built_at_all() {
        assert_eq!(
            Value::parse("two\nlines"),
            Err(ValueError::Newline { index: 3 })
        );
    }
}
