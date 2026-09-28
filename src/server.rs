use crate::clock::now_ns;
use crate::proto::{Request, Response};
use std::cell::Cell;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::panic;
use std::thread;
use std::time::Duration;
use woonsocket_work::Work;
use woonsocket_work::args::WoonsocketServerOpt;

pub fn run(opt: WoonsocketServerOpt) -> std::io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", opt.port))?;
    thread::spawn(move || serve(listener));
    // Blocking accept/read can't observe a deadline, and the server has nothing to flush.
    thread::sleep(Duration::from_secs(opt.runtime_secs));
    std::process::exit(0);
}

/// Accept connections forever, one thread per connection.
pub fn serve(listener: TcpListener) {
    silence_perform_panics();
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                thread::spawn(move || {
                    if let Err(e) = handle_conn(s) {
                        eprintln!("connection closed: {e}");
                    }
                });
            }
            Err(e) => eprintln!("accept failed: {e}"),
        }
    }
}

fn handle_conn(stream: TcpStream) -> bincode::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);
    while let Some(req) = recv(&mut reader)? {
        let server_recv_ns = now_ns();
        let payload = perform(req.work);
        let resp = Response {
            id: req.id,
            server_recv_ns,
            server_sent_ns: now_ns(),
            payload,
        };
        bincode::serialize_into(&mut writer, &resp)?;
        writer.flush()?; // one write() per response
    }
    Ok(())
}

/// Read one request; `Ok(None)` if the client closed the connection cleanly.
fn recv(r: &mut impl Read) -> bincode::Result<Option<Request>> {
    match bincode::deserialize_from(r) {
        Ok(req) => Ok(Some(req)),
        Err(e) => match *e {
            bincode::ErrorKind::Io(ref io) if io.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            _ => Err(e),
        },
    }
}

thread_local! {
    static IN_PERFORM: Cell<bool> = const { Cell::new(false) };
}

/// `Work::Poisson` panics when its draw is 0; that draw means no work, so treat it as such.
fn perform(work: Work) -> Vec<u8> {
    IN_PERFORM.set(true);
    let out = panic::catch_unwind(move || work.perform());
    IN_PERFORM.set(false);
    out.ok().flatten().unwrap_or_default()
}

/// Suppress the panic message for panics caught in `perform`, keeping it for everything else.
fn silence_perform_panics() {
    let default = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        if !IN_PERFORM.get() {
            default(info);
        }
    }));
}
