use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use woonsocket_work::Work;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub work: Work,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    pub server_recv_ns: u64,
    pub server_sent_ns: u64,
    pub payload: Vec<u8>,
}

/// `writer` should be buffered; the flush makes each message a single write.
pub fn send<T: Serialize>(writer: &mut impl Write, message: &T) -> io::Result<()> {
    bincode::serialize_into(&mut *writer, message).map_err(io::Error::other)?;
    writer.flush()
}

/// `None` once the peer closes the connection. `reader` should be buffered.
pub fn recv<T: DeserializeOwned>(reader: &mut impl Read) -> io::Result<Option<T>> {
    match bincode::deserialize_from(reader) {
        Ok(message) => Ok(Some(message)),
        Err(e) => match *e {
            bincode::ErrorKind::Io(io) if io.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            bincode::ErrorKind::Io(io) => Err(io),
            other => Err(io::Error::other(other)),
        },
    }
}
