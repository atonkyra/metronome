extern crate prometheus_exporter_base;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate rmp_serde as rmps;
extern crate clap;
use lib::datatypes::ServerStatistics;
use std::collections::HashMap;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::net::UdpSocket;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use clap::{Arg, App};
use std::thread;

mod lib;

fn socket_thread(_config: lib::datatypes::ServerConfig, socket_raw: UdpSocket, running: Arc<AtomicBool>, received: std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, transmit: std::sync::mpsc::Receiver<lib::datatypes::WrappedMessage>) {
    let mut rxbuf = [0;65535];
    let socket = Arc::new(socket_raw);
    let socket_tx_thread = socket.clone();
    let socket_rx_thread = socket.clone();
    let running_tx_thread = running.clone();
    let running_rx_thread = running.clone();

    let tx_thread = thread::spawn(move || {
        while running_tx_thread.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok(transmittable_message) = transmit.recv() {
                let mut buf = Vec::new();
                if let Ok(_serialized_message) = transmittable_message.message.serialize(&mut Serializer::new(&mut buf)) {
                    if let Err(_send_error) = socket_tx_thread.send_to(&buf, transmittable_message.addr) {
                        // TODO: log
                    }
                };
            } else {
                break;
            }
        }
    });

    let rx_thread = thread::spawn(move || {
        while running_rx_thread.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok((_size, addr)) = socket_rx_thread.recv_from(&mut rxbuf) {
                let mut deserializer = Deserializer::from_slice(&rxbuf);
                let rxmsg_result : Result<lib::datatypes::MetronomeMessage, _> = Deserialize::deserialize(&mut deserializer);
                if let Ok(received_message) = rxmsg_result {
                    let wrapped_message = lib::datatypes::WrappedMessage {
                        addr: addr,
                        message: received_message,
                    };
                    if let Err(_result) = received.send(wrapped_message) {}
                }
            }
        }

    });

    let _ = tx_thread.join();
    let _ = rx_thread.join();
}

fn handle_message(config: &lib::datatypes::ServerConfig, wrapped_message: lib::datatypes::WrappedMessage, to_socket: &std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, client_stats: &mut Option<String>) -> bool {
    let message = wrapped_message.message;
    if message.key != config.key {
        return false;
    }
    if message.mode == "ping" {
        if let Some(mut payload) = message.payload {
            if message.mul != 1.0 {
                let target_len : usize = ((payload.len() as f32) * message.mul) as usize;
                payload = std::iter::repeat(payload.chars().next().unwrap()).take(target_len).collect::<String>();
            }
            let reply_message = lib::datatypes::WrappedMessage {
                addr: wrapped_message.addr,
                message: lib::datatypes::MetronomeMessage {
                    mode: "pong".to_string(),
                    payload: Some(payload),
                    seq: message.seq,
                    key: message.key,
                    mul: message.mul,
                    sid: message.sid,
                }
            };
            if let Err(_result) = to_socket.send(reply_message) {}
            return true;
        }
    } else if message.mode == "stat" {
        if let Some(client_stat_payload) = message.payload {
            *client_stats = Some(client_stat_payload);
            return true;
        }
    }
    return false;
}

fn handler_thread(config: lib::datatypes::ServerConfig, running: Arc<AtomicBool>, to_socket: std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, from_socket: std::sync::mpsc::Receiver<lib::datatypes::WrappedMessage>, to_stats: std::sync::mpsc::Sender<Vec<lib::datatypes::ServerStatistics>>) {
    let mut sessions: HashMap<String, ServerStatistics> = HashMap::new();
    let mut last_stats_emitted: f64 = 0.0;
    while running.load(std::sync::atomic::Ordering::Relaxed) {
        let mut client_stats: Option<String> = None;
        let cur_time = lib::util::get_precise_time();

        if let Ok(message_from_socket) = from_socket.recv_timeout(Duration::from_millis(100)) {
            let sid_cloned = message_from_socket.message.sid.clone();
            let message_ok = handle_message(&config, message_from_socket, &to_socket, &mut client_stats);
            if message_ok {
                if let Some(session_stats) = sessions.get_mut(&sid_cloned) {
                    if let Some(client_stats) = client_stats {
                        session_stats.client_stats = client_stats;
                    } else {
                        session_stats.last_rx = cur_time;
                        session_stats.recv += 1;
                    }
                } else if client_stats.is_none() {
                    let ss = ServerStatistics {
                        sid: sid_cloned.clone(),
                        last_rx: cur_time,
                        recv: 1,
                        client_stats: "".to_string(),
                    };
                    sessions.insert(sid_cloned.clone(), ss);
                }
            }
        }
        if cur_time - last_stats_emitted > 1.0 {
            let mut emit_stats = Vec::new();
            let mut remove_keys = Vec::new();
            for (key, value) in sessions.iter() {
                if cur_time - value.last_rx > 5.0 {
                    remove_keys.push(key.clone());
                }
                emit_stats.push(value.clone());
            }
            for removable in remove_keys {
                sessions.remove(&removable);
            }
            if let Err(_) = to_stats.send(emit_stats) {
                // TODO: log
            } else {
                last_stats_emitted = cur_time;
            }
        }
    }
}

fn stats_thread(config: lib::datatypes::ServerConfig, running: Arc<AtomicBool>, stats_receiver: std::sync::mpsc::Receiver<Vec<lib::datatypes::ServerStatistics>>) {
    let stats_rx_channel_mutexed = Arc::new(Mutex::new(stats_receiver));
    let prometheus_data = Arc::new(Mutex::new("".to_string()));
    let prometheus_data_exporter = prometheus_data.clone();
    let prometheus_data_receiver = prometheus_data.clone();

    let stats_receiver = std::thread::spawn(move || {
        while running.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok(stats_rx_channel) = stats_rx_channel_mutexed.lock() {
                if let Ok(received_stats_from_channel) = stats_rx_channel.recv() {
                    let mut stats_string = "".to_string();
                    let prom_recv = prometheus_exporter_base::PrometheusMetric::new(
                        "metronome_server_recv", prometheus_exporter_base::MetricType::Counter, "recv packets"
                    );
                    prom_recv.render_header();
                    for session_stat in received_stats_from_channel {
                        let mut attr = Vec::new();
                        attr.push(("sid", session_stat.sid.as_str()));
                        stats_string.push_str(&prom_recv.render_sample(Some(&attr), session_stat.recv));
                        stats_string.push_str(session_stat.client_stats.as_str());
                    }
                    if let Ok(mut prometheus_data_receiver) = prometheus_data_receiver.lock() {
                        *prometheus_data_receiver = stats_string;
                    }
                }
            }
        }
    });

    prometheus_exporter_base::render_prometheus(config.prometheus, lib::datatypes::ExporterOptions::default(), |_request, _options| {
        async move {
            if let Ok(prometheus_data_exporter) = prometheus_data_exporter.lock() {
                let prometheus_data_out: String = prometheus_data_exporter.clone();
                Ok(prometheus_data_out)
            } else {
                let no_stats = "".to_string();
                Ok(no_stats)
            }
        }
    });

    stats_receiver.join().unwrap();
}

fn main() {
    let matches = App::new("metronome-server")
        .version("0.0")
        .arg(
            Arg::with_name("bind")
                .short("b")
                .long("bind")
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
                .short("P")
                .long("prometheus")
                .takes_value(true)
                .default_value("0.0.0.0:9150")
        )
        .get_matches();

    let config = lib::datatypes::ServerConfig {
        bind: matches.value_of("bind").unwrap().parse().unwrap(),
        key: matches.value_of("key").unwrap().to_string(),
        prometheus: matches.value_of("prometheus").unwrap().parse().unwrap(),
    };

    let socket;
    match UdpSocket::bind(config.bind) {
        Ok(bound_socket) => {
            socket = bound_socket;
        },
        Err(_) => {
            return;
        }
    }
    if let Err(_) = socket.set_read_timeout(Some(Duration::from_millis(100))) {
        eprintln!("failed to set socket read timeout!");
        return;
    }
    let running = Arc::new(AtomicBool::new(true));
    let (socket_thd_rx, socket_rx) = channel();
    let (socket_tx, socket_thd_tx) = channel();
    let (stats_tx, stats_rx) = channel();
    let sock_thread_running = running.clone();
    let handler_thread_running = running.clone();
    let stats_thread_running = running.clone();
    let config_socket_thread = config.clone();
    let config_handler_thread = config.clone();
    let config_stats_thread = config.clone();
    let sock_thread = std::thread::spawn(|| {
        socket_thread(config_socket_thread, socket, sock_thread_running, socket_thd_rx, socket_thd_tx);
    });
    let handler_thread = std::thread::spawn(|| {
        handler_thread(config_handler_thread, handler_thread_running, socket_tx, socket_rx, stats_tx);
    });
    let stats_thread = std::thread::spawn(|| {
        stats_thread(config_stats_thread, stats_thread_running, stats_rx);
    });
    sock_thread.join().unwrap();
    handler_thread.join().unwrap();
    stats_thread.join().unwrap();
}
