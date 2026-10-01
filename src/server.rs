use crate::clock::now_ns;
use crate::proto::{Request, Response, recv, send};
use std::cell::Cell;
use std::io::{self, BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::time::Duration;
use std::{panic, process, thread};
use woonsocket_work::Work;

pub fn run(port: u16, runtime: Duration) -> io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port))?;
    silence_perform_panics();
    thread::spawn(move || {
        for stream in listener.incoming() {
            if let Err(e) = stream.and_then(handle_connection) {
                eprintln!("Failed to accept connection: {e}");
            }
        }
    });
    // Blocking accepts and reads can't observe a deadline, and there is nothing to flush.
    thread::sleep(runtime);
    process::exit(0)
}

/// A reader drains requests off the socket as fast as they arrive, so overload queues up here
/// rather than pushing back on the client; a worker serves the queue in order.
fn handle_connection(stream: TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);
    let (queue, requests) = mpsc::channel();

    thread::spawn(move || {
        loop {
            let request = match recv::<Request>(&mut reader) {
                Ok(Some(request)) => request,
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Failed to receive message: {e}");
                    break;
                }
            };
            if queue.send((request, now_ns())).is_err() {
                break;
            }
        }
    });

    // Ends once the reader is done and the queue is drained; dropping the writer then closes
    // the connection, which tells the client every response has been sent.
    thread::spawn(move || {
        for (request, server_recv_ns) in requests {
            let payload = perform(request.work);
            let response = Response {
                id: request.id,
                server_recv_ns,
                server_sent_ns: now_ns(),
                payload,
            };
            if let Err(e) = send(&mut writer, &response) {
                eprintln!("Failed to send message: {e}");
                break;
            }
        }
    });
    Ok(())
}

thread_local! {
    static IN_PERFORM: Cell<bool> = const { Cell::new(false) };
}

/// `Work::Poisson` panics when it draws 0; that draw means no work, so treat it as such.
fn perform(work: Work) -> Vec<u8> {
    IN_PERFORM.set(true);
    let result = panic::catch_unwind(|| work.perform());
    IN_PERFORM.set(false);
    result.ok().flatten().unwrap_or_default()
}

/// Hides the message for panics caught in `perform`; every other panic still reports.
fn silence_perform_panics() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        if !IN_PERFORM.get() {
            default_hook(info);
        }
    }));
}
