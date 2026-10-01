use serde::Serialize;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SendEntry {
    pub scheduled_ns: u64,
    pub client_sent_ns: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecvEntry {
    pub client_recv_ns: u64,
    pub server_recv_ns: u64,
    pub server_sent_ns: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Record {
    pub client_label: u16,
    pub id: u64,
    pub scheduled_ns: u64,
    pub client_sent_ns: u64,
    pub client_recv_ns: u64,
    pub server_recv_ns: u64,
    pub server_sent_ns: u64,
}
