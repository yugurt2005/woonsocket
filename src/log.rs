#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SendEntry {
    pub scheduled_ns: u64,
    pub client_sent_ns: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecvEntry {
    pub id: u64,
    pub client_recv_ns: u64,
    pub server_recv_ns: u64,
    pub server_sent_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record {
    pub client_id: u32,
    pub id: u64,
    pub scheduled_ns: u64,
    pub client_sent_ns: u64,
    pub client_recv_ns: Option<u64>,
    pub server_recv_ns: Option<u64>,
    pub server_sent_ns: Option<u64>,
}
