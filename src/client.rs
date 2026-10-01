use crate::clock::now_ns;
use crate::log::{Record, RecvEntry, SendEntry};
use crate::proto::{Request, Response, recv, send};
use crate::settings::{Mode, OpenLoopKind, RunSettings};
use rand::Rng;
use rand_distr::Exp1;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::iter::zip;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;

/// Stop this long before `duration` ends so joining and saving finish within it.
const DRAIN_BUDGET: Duration = Duration::from_millis(250);

fn save_run_to_disk(settings: &RunSettings, records: &[Record]) -> io::Result<()> {
    fs::create_dir_all(&settings.outpath)?;

    let file = File::create(settings.outpath.join("settings.json"))?;
    serde_json::to_writer_pretty(file, settings)?;

    let mut writer = BufWriter::new(File::create(settings.outpath.join("records.bin"))?);
    for record in records {
        bincode::serialize_into(&mut writer, record).map_err(io::Error::other)?;
    }
    writer.flush()
}

fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
    match TcpStream::connect(addr) {
        Ok(stream) => {
            stream.set_nodelay(true)?;
            return Ok(stream);
        }
        Err(err) => {
            panic!("Failed to connect to {}: {}", addr, err);
        }
    }
}

fn join(client_label: u16, sends: &[SendEntry], recvs: &[RecvEntry]) -> Vec<Record> {
    zip(sends, recvs)
        .enumerate()
        .map(|(id, (s, r))| Record {
            client_label,
            id: id as u64,
            scheduled_ns: s.scheduled_ns,
            client_sent_ns: s.client_sent_ns,
            client_recv_ns: r.client_recv_ns,
            server_recv_ns: r.server_recv_ns,
            server_sent_ns: r.server_sent_ns,
        })
        .collect()
}

/// Waits until `done()` or the deadline, then shuts down `streams`, which unblocks any thread
/// still blocked reading or writing on them.
fn shutdown_at_deadline(streams: &[TcpStream], deadline_ns: u64, done: impl Fn() -> bool) {
    while !done() && now_ns() < deadline_ns {
        thread::sleep(Duration::from_millis(1));
    }
    for stream in streams {
        stream.shutdown(Shutdown::Both).ok();
    }
}

fn run_closed_loop(
    settings: &RunSettings,
    deadline_ns: u64,
) -> io::Result<Vec<Record>> {
    let streams = (0..settings.num_threads)
        .map(|_| connect(settings.addr))
        .collect::<io::Result<Vec<_>>>()?;

    let workers = streams
        .iter()
        .enumerate()
        .map(|(label, stream)| {
            let mut writer = BufWriter::new(stream.try_clone()?);
            let mut reader = BufReader::new(stream.try_clone()?);
            let work = settings.work;
            Ok(thread::spawn(move || {
                let mut sends = Vec::new();
                let mut recvs = Vec::new();
                while now_ns() < deadline_ns {
                    let request = Request {
                        id: sends.len() as u64,
                        work,
                    };
                    let client_sent_ns = now_ns();
                    if let Err(e) = send(&mut writer, &request) {
                        eprintln!("Failed to send message: {e}");
                        break;
                    }
                    sends.push(SendEntry {
                        scheduled_ns: 0,
                        client_sent_ns,
                    });

                    let response = match recv::<Response>(&mut reader) {
                        Ok(Some(response)) => response,
                        Ok(None) => break,
                        Err(e) => {
                            eprintln!("Failed to receive message: {e}");
                            break;
                        }
                    };
                    recvs.push(RecvEntry {
                        client_recv_ns: now_ns(),
                        server_recv_ns: response.server_recv_ns,
                        server_sent_ns: response.server_sent_ns,
                    });
                }
                recvs.resize(sends.len(), RecvEntry::default());
                join(label as u16, &sends, &recvs)
            }))
        })
        .collect::<io::Result<Vec<_>>>()?;

    shutdown_at_deadline(&streams, deadline_ns, || {
        workers.iter().all(|w| w.is_finished())
    });
    Ok(workers
        .into_iter()
        .flat_map(|w| w.join().expect("closed-loop worker panicked"))
        .collect())
}

/// One connection: this thread sends on schedule, a spawned thread receives.
/// `sample(interval)` gives the time from one scheduled send to the next.
/// The caller shuts `stream` down at the deadline.
fn run_single_open_loop(
    settings: &RunSettings,
    stream: &TcpStream,
    label: u16,
    interval: Duration,
    sample: fn(Duration) -> Duration,
    deadline_ns: u64,
) -> io::Result<Vec<Record>> {
    let mut writer = BufWriter::new(stream.try_clone()?);
    let mut reader = BufReader::new(stream.try_clone()?);

    let receiver = thread::spawn(move || {
        let mut recvs = Vec::new();
        loop {
            let response = match recv::<Response>(&mut reader) {
                Ok(Some(response)) => response,
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Failed to receive message: {e}");
                    break;
                }
            };
            let client_recv_ns = now_ns();
            let id = response.id as usize;
            if id >= recvs.len() {
                recvs.resize(id + 1, RecvEntry::default());
            }
            recvs[id] = RecvEntry {
                client_recv_ns,
                server_recv_ns: response.server_recv_ns,
                server_sent_ns: response.server_sent_ns,
            };
        }
        recvs
    });

    let mut sends = Vec::new();
    let mut scheduled_ns = now_ns();
    loop {
        scheduled_ns += sample(interval).as_nanos() as u64;
        if scheduled_ns >= deadline_ns {
            break;
        }
        while now_ns() < scheduled_ns {
            std::hint::spin_loop();
        }
        if now_ns() >= deadline_ns {
            break;
        }
        let request = Request {
            id: sends.len() as u64,
            work: settings.work,
        };
        let client_sent_ns = now_ns();
        if let Err(e) = send(&mut writer, &request) {
            eprintln!("Failed to send message: {e}");
            break;
        }
        sends.push(SendEntry {
            scheduled_ns,
            client_sent_ns,
        });
    }

    stream.shutdown(Shutdown::Write).ok();
    let mut recvs = receiver.join().expect("open-loop receiver panicked");
    recvs.resize(sends.len(), RecvEntry::default());
    Ok(join(label, &sends, &recvs))
}

fn run_open_loop(
    settings: &RunSettings,
    interval: Duration,
    sample: fn(Duration) -> Duration,
    deadline_ns: u64,
) -> io::Result<Vec<Record>> {
    let streams = (0..settings.num_threads)
        .map(|_| connect(settings.addr))
        .collect::<io::Result<Vec<_>>>()?;

    thread::scope(|scope| {
        let workers: Vec<_> = streams
            .iter()
            .enumerate()
            .map(|(label, stream)| {
                scope.spawn(move || {
                    run_single_open_loop(
                        settings,
                        stream,
                        label as u16,
                        interval,
                        sample,
                        deadline_ns,
                    )
                })
            })
            .collect();
        shutdown_at_deadline(&streams, deadline_ns, || {
            workers.iter().all(|w| w.is_finished())
        });
        let mut records = Vec::new();
        for worker in workers {
            records.extend(worker.join().expect("open-loop worker panicked")?);
        }
        Ok(records)
    })
}

fn run_open_loop_constant(
    settings: &RunSettings,
    interval: Duration,
    deadline_ns: u64,
) -> io::Result<Vec<Record>> {
    run_open_loop(settings, interval, |interval| interval, deadline_ns)
}

/// Poisson arrivals have exponentially distributed intervals.
fn run_open_loop_poisson(
    settings: &RunSettings,
    interval: Duration,
    deadline_ns: u64,
) -> io::Result<Vec<Record>> {
    run_open_loop(
        settings,
        interval,
        |interval| interval.mul_f64(rand::thread_rng().sample(Exp1)),
        deadline_ns,
    )
}

pub fn run(settings: &RunSettings) -> io::Result<()> {
    let deadline_ns =
        now_ns() + settings.duration.saturating_sub(DRAIN_BUDGET).as_nanos() as u64;
    let records = match settings.mode {
        Mode::ClosedLoop => run_closed_loop(settings, deadline_ns)?,
        Mode::OpenLoop {
            interval,
            kind: OpenLoopKind::Constant,
        } => run_open_loop_constant(settings, interval, deadline_ns)?,
        Mode::OpenLoop {
            interval,
            kind: OpenLoopKind::Poisson,
        } => run_open_loop_poisson(settings, interval, deadline_ns)?,
    };
    save_run_to_disk(&settings, &records)
}
