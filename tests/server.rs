use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::num::NonZeroU64;
use std::thread;
use woonsocket::proto::{Request, Response};
use woonsocket_work::Work;

fn start_server() -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || woonsocket::server::serve(listener));
    TcpStream::connect(addr).unwrap()
}

fn nz(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}

#[test]
fn serves_all_work_kinds_pipelined() {
    let stream = start_server();
    let mut writer = BufWriter::new(stream.try_clone().unwrap());
    let mut reader = BufReader::new(stream);

    // poisson:1 draws 0 (and panics inside the work crate) ~37% of the time.
    let mut works = vec![Work::Immediate, Work::Const(nz(1000)), Work::Payload];
    works.extend(std::iter::repeat_n(Work::Poisson(nz(1)), 50));

    // Send everything before reading anything, like the open-loop client.
    for (id, &work) in works.iter().enumerate() {
        bincode::serialize_into(&mut writer, &Request { id: id as u64, work }).unwrap();
    }
    writer.flush().unwrap();

    for (id, work) in works.iter().enumerate() {
        let resp: Response = bincode::deserialize_from(&mut reader).unwrap();
        assert_eq!(resp.id, id as u64);
        assert!(resp.server_recv_ns <= resp.server_sent_ns);
        match work {
            Work::Payload => assert!([64, 256, 512, 1024].contains(&resp.payload.len())),
            _ => assert!(resp.payload.is_empty()),
        }
    }
}

#[test]
fn survives_client_disconnect() {
    let stream = start_server();
    let addr = stream.peer_addr().unwrap();
    drop(stream);

    // A fresh connection to the same server still works.
    let stream = TcpStream::connect(addr).unwrap();
    let mut writer = BufWriter::new(stream.try_clone().unwrap());
    let mut reader = BufReader::new(stream);
    bincode::serialize_into(&mut writer, &Request { id: 7, work: Work::Immediate }).unwrap();
    writer.flush().unwrap();
    let resp: Response = bincode::deserialize_from(&mut reader).unwrap();
    assert_eq!(resp.id, 7);
}
