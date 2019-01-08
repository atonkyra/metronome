extern crate time;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate rmp_serde as rmps;
use std::net::{UdpSocket, SocketAddr};
use std::sync::mpsc::channel;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
extern crate clap;
use clap::{Arg, App, SubCommand};

mod lib;

fn socket_thread(socket: UdpSocket, running: Arc<AtomicBool>, received: std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, transmit: std::sync::mpsc::Receiver<lib::datatypes::WrappedMessage>) {
    let zero_duration = Duration::from_secs(0);
    let sleep_duration = Duration::from_millis(1);
    let mut work_done;
    let mut rxbuf = [0;65535];
    while running.load(std::sync::atomic::Ordering::Relaxed) {
        work_done = false;
        if let Ok(transmittable_message) = transmit.recv_timeout(zero_duration) {
            let mut buf = Vec::new();
            if let Ok(_serialized_message) = transmittable_message.message.serialize(&mut Serializer::new(&mut buf)) {
                if let Err(_send_error) = socket.send_to(&buf, transmittable_message.addr) {
                    // TODO: log
                }
            };
            work_done = true;
        }
        if let Ok((_size, addr)) = socket.recv_from(&mut rxbuf) {
            let mut deserializer = Deserializer::from_slice(&rxbuf);
            let rxmsg_result : Result<lib::datatypes::MetronomeMessage, _> = Deserialize::deserialize(&mut deserializer);
            if let Ok(received_message) = rxmsg_result {
                // TODO: verify that it is ok to receive (key match)
                let wrapped_message = lib::datatypes::WrappedMessage {
                    addr: addr,
                    message: received_message,
                };
                if let Err(_result) = received.send(wrapped_message) {}
            }
            work_done = true;
        }
        
        if !work_done {
            thread::sleep(sleep_duration);
        }
    }
}

fn handle_message(inflight: &mut HashMap<u64, lib::datatypes::PingResult>, wrapped_message: lib::datatypes::WrappedMessage, _to_socket: &std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, stats: &mut lib::datatypes::Statistics) {
    let cur_time = lib::util::get_time();
    if let Some((_key, inflight_pingresult)) = inflight.remove_entry(&wrapped_message.message.seq) {
        //println!("<- {} seq={}, rtt={}s", wrapped_message.message.mode, wrapped_message.message.seq, cur_time - inflight_pingresult.timestamp);
        stats.recv += 1;
        let timediff = cur_time - inflight_pingresult.timestamp;
        if stats.rtt_mavg == std::f64::INFINITY {
            stats.rtt_mavg = timediff;
        } else {
            stats.rtt_mavg = (stats.rtt_mavg * 9.0 + timediff) / 10.0;
        }
        if stats.rtt_best > timediff {
            stats.rtt_best = timediff;
        }
        if stats.rtt_worst < timediff {
            stats.rtt_worst = timediff;
        }
    } else {
        println!("!- {} seq={}, duplicate/late", wrapped_message.message.mode, wrapped_message.message.seq);
    }
}

fn scan_deadlines(inflight: &mut HashMap<u64, lib::datatypes::PingResult>, stats: &mut lib::datatypes::Statistics) {
    let cur_time = lib::util::get_time();
    let mut expired: Vec<u64> = Vec::new();
    for (seq, item) in inflight.iter() {
        if cur_time > item.deadline {
            expired.push(*seq);
        }
    }
    for expired_seq in expired.iter() {
        println!("!! timeout seq={}", expired_seq);
        inflight.remove(expired_seq);
        stats.lost += 1;
    }
}

fn handler_thread(config: lib::datatypes::ClientConfig, running: Arc<AtomicBool>, to_socket: std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, from_socket: std::sync::mpsc::Receiver<lib::datatypes::WrappedMessage>) {
    let mut inflight: HashMap<u64, lib::datatypes::PingResult> = HashMap::new();
    let zero_duration = Duration::from_secs(0);
    let sleep_duration = Duration::from_micros(100);
    let mut work_done;

    let mut msg_seq: u64 = 0;
    let mut last_msg_sent_precise : f64 = 0.0;
    let mut last_report: f64 = 0.0;

    let mut stats = lib::datatypes::Statistics::new();

    let remote = SocketAddr::from(([127,0,0,1],13337));

    let pps_sleeptime = 1.0/(config.pps_limit as f64);

    while running.load(std::sync::atomic::Ordering::Relaxed) {
        work_done = false;
        if let Ok(message_from_socket) = from_socket.recv_timeout(zero_duration) {
            handle_message(&mut inflight, message_from_socket, &to_socket, &mut stats);
            work_done = true;
        }
        
        let cur_time = lib::util::get_time();
        let cur_precise_time = lib::util::get_precise_time();

        if (cur_precise_time - last_msg_sent_precise) > pps_sleeptime {
            let msg = lib::datatypes::WrappedMessage {
                addr: remote,
                message: lib::datatypes::MetronomeMessage {
                    mode: "ping".to_string(),
                    payload: Some("payload".to_string()),
                    seq: msg_seq,
                }
            };
            if let Ok(_) = to_socket.send(msg) {
                inflight.insert(msg_seq, lib::datatypes::PingResult {
                    deadline: cur_time + 1.0,
                    timestamp: cur_time,
                });
                msg_seq += 1;
                stats.sent += 1;
                last_msg_sent_precise = cur_precise_time;
            }
            work_done = true;
        }

        if cur_time - last_report > 1.0 {
            println!("@ seq {}: {} sent, {} recv, {} lost | rtt avg {:.4}, best {:.4}, worst {:.4}", msg_seq, stats.sent, stats.recv, stats.lost, stats.rtt_mavg, stats.rtt_best, stats.rtt_worst);
            last_report = cur_time;
        }

        scan_deadlines(&mut inflight, &mut stats);
        if !work_done {
            thread::sleep(sleep_duration);
        }
    }
}

fn main() {
    let matches = App::new("metronome-client")
        .version("0.0")
        .arg(
            Arg::with_name("pps-max")
                .short("p")
                .long("pps-max")
                .takes_value(true)
                .default_value("1")
        )
        .arg(
            Arg::with_name("payload-size")
                .short("s")
                .long("payload-size")
                .takes_value(true)
                .default_value("1")
        )
        .arg(
            Arg::with_name("balance")
                .short("b")
                .long("balance")
                .takes_value(true)
                .default_value("1")
        )
        .get_matches();

    let config = lib::datatypes::ClientConfig {
        pps_limit: matches.value_of("pps-max").unwrap().parse().unwrap(),
        payload_size: matches.value_of("payload-size").unwrap().parse().unwrap(),
        balance: matches.value_of("balance").unwrap().parse().unwrap(),
    };

    let socket;
    match UdpSocket::bind("0.0.0.0:0") {
        Ok(bound_socket) => {
            socket = bound_socket;
        },
        Err(_) => {
            return;
        }
    }
    if let Err(_) = socket.set_nonblocking(true) {
        // TODO: log
        return;
    }
    let running = Arc::new(AtomicBool::new(true));
    let (socket_thd_rx, socket_rx) = channel();
    let (socket_tx, socket_thd_tx) = channel();
    let sock_thread_running = running.clone();
    let sock_thread = std::thread::spawn(|| {
        socket_thread(socket, sock_thread_running, socket_thd_rx, socket_thd_tx);
    });
    let handler_thread_running = running.clone();
    let handler_thread = std::thread::spawn(|| {
        handler_thread(config, handler_thread_running, socket_tx, socket_rx);
    });
    sock_thread.join().unwrap();
    handler_thread.join().unwrap();
}
