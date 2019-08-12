extern crate time;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate rmp_serde as rmps;
extern crate tempfile;
use std::fs::Permissions;
use std::os::unix::prelude::*;
use std::io::Write;
use std::net::{UdpSocket};
use std::sync::mpsc::channel;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
extern crate clap;
use clap::{Arg, App};

mod lib;

fn socket_thread(config: lib::datatypes::ClientConfig, socket: UdpSocket, running: Arc<AtomicBool>, received: std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, transmit: std::sync::mpsc::Receiver<lib::datatypes::WrappedMessage>) {
    let mut rxbuf = [0;65535];
    while running.load(std::sync::atomic::Ordering::Relaxed) {
        if let Ok(transmittable_message) = transmit.try_recv() {
            let mut buf = Vec::new();
            if let Ok(_serialized_message) = transmittable_message.message.serialize(&mut Serializer::new(&mut buf)) {
                if let Err(_send_error) = socket.send_to(&buf, transmittable_message.addr) {
                    // TODO: log
                }
            };
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
        }
        if config.use_sleep {
            let sleeptime = std::time::Duration::from_micros(100);
            std::thread::sleep(sleeptime);
        }
    }
}

fn handle_message(config: &lib::datatypes::ClientConfig, inflight: &mut HashMap<u64, lib::datatypes::PingResult>, wrapped_message: lib::datatypes::WrappedMessage, _to_socket: &std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, stats: &mut lib::datatypes::Statistics) {
    if wrapped_message.message.key != config.key { return; }
    let cur_time = lib::util::get_time();
    if let Some((_key, inflight_pingresult)) = inflight.remove_entry(&wrapped_message.message.seq) {
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
        inflight.remove(expired_seq);
        stats.lost += 1;
    }
}

fn update_statistics(config: lib::datatypes::ClientConfig, running: Arc<AtomicBool>, stats_from_handler: std::sync::mpsc::Receiver<lib::datatypes::Statistics>) {
    while running.load(std::sync::atomic::Ordering::Relaxed) {
        if let Ok(stats) = stats_from_handler.recv() {
            let mut file = tempfile::NamedTempFile::new_in(&config.prometheus).unwrap();
            let _ = writeln!(file, "metronome_sent {}", stats.sent);
            let _ = writeln!(file, "metronome_recv {}", stats.recv);
            let _ = writeln!(file, "metronome_lost {}", stats.lost);
            if stats.rtt_mavg != std::f64::INFINITY {
                let _ = writeln!(file, "metronome_rtt_mavg {}", stats.rtt_mavg);
            }
            let path = file.path().to_str().unwrap();
            let _ = std::fs::set_permissions(path, Permissions::from_mode(0o744));
            if let Ok(_persist_info) = file.persist(format!("{}/{}", &config.prometheus, "metronome.prom")) {
            } else {
                println!("error writing prometheus file");
            }
        }
    }
}

fn handler_thread(config: lib::datatypes::ClientConfig, running: Arc<AtomicBool>, to_socket: std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, from_socket: std::sync::mpsc::Receiver<lib::datatypes::WrappedMessage>, to_stats: std::sync::mpsc::Sender<lib::datatypes::Statistics>) {
    let mut inflight: HashMap<u64, lib::datatypes::PingResult> = HashMap::new();

    let mut msg_seq: u64 = 0;
    let mut last_msg_sent_precise : f64 = 0.0;
    let mut last_report: f64 = 0.0;

    let mut stats = lib::datatypes::Statistics::new();

    let pps_sleeptime = 1.0/(config.pps_limit as f64);

    let payload = std::iter::repeat("X").take(config.payload_size).collect::<String>();

    while running.load(std::sync::atomic::Ordering::Relaxed) {
        if let Ok(message_from_socket) = from_socket.try_recv() {
            handle_message(&config, &mut inflight, message_from_socket, &to_socket, &mut stats);
        }
        
        let cur_time = lib::util::get_time();
        let cur_precise_time = lib::util::get_precise_time();

        if (cur_precise_time - last_msg_sent_precise) > pps_sleeptime {
            let msg = lib::datatypes::WrappedMessage {
                addr: config.remote,
                message: lib::datatypes::MetronomeMessage {
                    mode: "ping".to_string(),
                    payload: Some(payload.clone()),
                    seq: msg_seq,
                    key: config.key.clone(),
                    mul: config.balance,
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
        }

        if cur_time - last_report > 1.0 {
            println!("@ seq {}: {} sent, {} recv, {} lost | rtt avg {:.4}, best {:.4}, worst {:.4}", msg_seq, stats.sent, stats.recv, stats.lost, stats.rtt_mavg, stats.rtt_best, stats.rtt_worst);
            last_report = cur_time;
        }

        scan_deadlines(&mut inflight, &mut stats);
        if last_report == cur_time && config.prometheus != "" {
            if let Err(e) = to_stats.send(stats.clone()) {
                println!("{}", e);
            }
        }

        if config.use_sleep {
            let sleeptime = std::time::Duration::from_micros(100);
            std::thread::sleep(sleeptime);
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
            Arg::with_name("use-sleep")
                .short("S")
                .long("use-sleep")
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
        .arg(
            Arg::with_name("remote")
                .short("r")
                .long("remote")
                .takes_value(true)
                .required(true)
        )
        .arg(
            Arg::with_name("key")
                .short("k")
                .long("key")
                .takes_value(true)
                .required(true)
        )
        .arg(
            Arg::with_name("prometheus")
                .long("prometheus")
                .takes_value(true)
                .default_value("")
        )
        .get_matches();

    let config = lib::datatypes::ClientConfig {
        pps_limit: matches.value_of("pps-max").unwrap().parse().unwrap(),
        payload_size: matches.value_of("payload-size").unwrap().parse().unwrap(),
        use_sleep: matches.is_present("use-sleep"),
        balance: matches.value_of("balance").unwrap().parse().unwrap(),
        remote: matches.value_of("remote").unwrap().parse().unwrap(),
        key: matches.value_of("key").unwrap().to_string(),
        prometheus: matches.value_of("prometheus").unwrap().to_string(),
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
    let (stats_tx, stats_rx) = channel();
    let sock_thread_running = running.clone();
    let socket_config = config.clone();
    let sock_thread = std::thread::spawn(|| {
        socket_thread(socket_config, socket, sock_thread_running, socket_thd_rx, socket_thd_tx);
    });
    let handler_thread_running = running.clone();
    let handler_config = config.clone();
    let handler_thread = std::thread::spawn(|| {
        handler_thread(handler_config, handler_thread_running, socket_tx, socket_rx, stats_tx);
    });
    let stats_thread_running = running.clone();
    let stats_config = config.clone();
    let stats_thread = std::thread::spawn(|| {
        update_statistics(stats_config, stats_thread_running, stats_rx);
    });
    sock_thread.join().unwrap();
    handler_thread.join().unwrap();
    stats_thread.join().unwrap();
}
