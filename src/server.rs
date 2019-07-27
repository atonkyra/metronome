extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate rmp_serde as rmps;
extern crate clap;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::net::UdpSocket;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use clap::{Arg, App};
use std::thread;

mod lib;

fn socket_thread(socket: UdpSocket, running: Arc<AtomicBool>, received: std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, transmit: std::sync::mpsc::Receiver<lib::datatypes::WrappedMessage>) {
    let sleep_duration = Duration::from_millis(1);
    let mut rxbuf = [0;65535];
    let mut last_packet_processed: f64 = 0.0;
    while running.load(std::sync::atomic::Ordering::Relaxed) {
        let cur_time = lib::util::get_time();
        if let Ok(transmittable_message) = transmit.try_recv() {
            let mut buf = Vec::new();
            if let Ok(_serialized_message) = transmittable_message.message.serialize(&mut Serializer::new(&mut buf)) {
                if let Err(_send_error) = socket.send_to(&buf, transmittable_message.addr) {
                    // TODO: log
                }
            };
            last_packet_processed = cur_time;
        }
        if let Ok((_size, addr)) = socket.recv_from(&mut rxbuf) {
            let mut deserializer = Deserializer::from_slice(&rxbuf);
            let rxmsg_result : Result<lib::datatypes::MetronomeMessage, _> = Deserialize::deserialize(&mut deserializer);
            if let Ok(received_message) = rxmsg_result {
                let wrapped_message = lib::datatypes::WrappedMessage {
                    addr: addr,
                    message: received_message,
                };
                if let Err(_result) = received.send(wrapped_message) {}
            }
            last_packet_processed = cur_time;
        }
        
        if cur_time - last_packet_processed > 5.0 {
            thread::sleep(sleep_duration);
        }
    }
}

fn handle_message(config: &lib::datatypes::ServerConfig, wrapped_message: lib::datatypes::WrappedMessage, to_socket: &std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>) {
    let message = wrapped_message.message;
    if message.key != config.key {
        return;
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
                }
            };
            if let Err(_result) = to_socket.send(reply_message) {}
        }
    }
}

fn handler_thread(config: lib::datatypes::ServerConfig, running: Arc<AtomicBool>, to_socket: std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, from_socket: std::sync::mpsc::Receiver<lib::datatypes::WrappedMessage>) {
    let sleep_duration = Duration::from_millis(1);
    let mut last_packet_processed: f64 = 0.0;
    while running.load(std::sync::atomic::Ordering::Relaxed) {
        let cur_time = lib::util::get_time();
        if let Ok(message_from_socket) = from_socket.try_recv() {
            handle_message(&config, message_from_socket, &to_socket);
            last_packet_processed = cur_time;
        }
        if cur_time - last_packet_processed > 5.0 {
            thread::sleep(sleep_duration);
        }
    }
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
        .get_matches();

    let config = lib::datatypes::ServerConfig {
        bind: matches.value_of("bind").unwrap().parse().unwrap(),
        key: matches.value_of("key").unwrap().to_string(),
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
