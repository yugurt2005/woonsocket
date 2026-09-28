use serde::{Deserialize, Serialize};
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
