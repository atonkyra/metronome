extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate rmp_serde as rmps;
extern crate clap;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Duration;
use std::net::UdpSocket;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use clap::{Arg, App};

mod lib;

fn socket_thread(socket: UdpSocket, running: Arc<AtomicBool>, received: std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, transmit: std::sync::mpsc::Receiver<lib::datatypes::WrappedMessage>) {
    let zero_duration = Duration::from_secs(0);
    let sleep_duration = Duration::from_micros(100);
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

fn handle_message(wrapped_message: lib::datatypes::WrappedMessage, to_socket: &std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>) {
    let message = wrapped_message.message;
    if message.mode == "ping" {
        if let Some(payload) = message.payload {
            let reply_message = lib::datatypes::WrappedMessage {
                addr: wrapped_message.addr,
                message: lib::datatypes::MetronomeMessage {
                    mode: "pong".to_string(),
                    payload: Some(payload),
                    seq: message.seq,
                }
            };
            if let Err(_result) = to_socket.send(reply_message) {}
        }
    }
}

fn handler_thread(running: Arc<AtomicBool>, to_socket: std::sync::mpsc::Sender<lib::datatypes::WrappedMessage>, from_socket: std::sync::mpsc::Receiver<lib::datatypes::WrappedMessage>) {
    let zero_duration = Duration::from_secs(0);
    let sleep_duration = Duration::from_micros(100);
    let mut work_done;
    while running.load(std::sync::atomic::Ordering::Relaxed) {
        work_done = false;
        if let Ok(message_from_socket) = from_socket.recv_timeout(zero_duration) {
            handle_message(message_from_socket, &to_socket);
            work_done = true;
        }
        if !work_done {
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
        .get_matches();

    let config = lib::datatypes::ServerConfig {
        bind: matches.value_of("bind").unwrap().parse().unwrap(),
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
        handler_thread(handler_thread_running, socket_tx, socket_rx);
    });
    sock_thread.join().unwrap();
    handler_thread.join().unwrap();
}
