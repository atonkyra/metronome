extern crate prometheus_exporter_base;
extern crate time;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate rmp_serde as rmps;
extern crate tempfile;
use std::net::{UdpSocket};
use std::sync::mpsc::channel;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;
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
    let stats_rx_channel_mutexed = Arc::new(Mutex::new(stats_from_handler));
    let prometheus_data = Arc::new(Mutex::new("".to_string()));
    let probe_id_cloned = config.probe_id.clone();
    let prometheus_data_exporter = prometheus_data.clone();
    let prometheus_data_receiver = prometheus_data.clone();
    let stats_receiver = std::thread::spawn(move || {
        let mut attributes = Vec::new();
        attributes.push((
            "probe_id", probe_id_cloned.as_str()
        ));
        while running.load(std::sync::atomic::Ordering::Relaxed) {
            let received_stats: Option<lib::datatypes::Statistics>;
            if let Ok(stats_rx_channel) = stats_rx_channel_mutexed.lock() {
                if let Ok(received_stats_from_channel) = stats_rx_channel.recv() {
                    received_stats = Some(received_stats_from_channel);
                } else {
                    received_stats = None;
                }
            } else {
                received_stats = None;
            }
            if let Some(stats) = received_stats {
                let mut stats_string = "".to_string();

                let prom_sent = prometheus_exporter_base::PrometheusMetric::new(
                    "metronome_sent", prometheus_exporter_base::MetricType::Counter, "sent packets"
                );
                prom_sent.render_header();
                stats_string.push_str(&prom_sent.render_sample(Some(&attributes), stats.sent));

                let prom_recv = prometheus_exporter_base::PrometheusMetric::new(
                    "metronome_recv", prometheus_exporter_base::MetricType::Counter, "recv packets"
                );
                prom_recv.render_header();
                stats_string.push_str(&prom_recv.render_sample(Some(&attributes), stats.recv));

                let prom_lost = prometheus_exporter_base::PrometheusMetric::new(
                    "metronome_lost", prometheus_exporter_base::MetricType::Counter, "lost packets"
                );
                prom_lost.render_header();
                stats_string.push_str(&prom_lost.render_sample(Some(&attributes), stats.lost));

                if stats.rtt_mavg != std::f64::INFINITY {
                    let prom_rtt_mavg = prometheus_exporter_base::PrometheusMetric::new(
                        "metronome_rtt_mavg", prometheus_exporter_base::MetricType::Gauge, "rtt mavg"
                    );
                    prom_rtt_mavg.render_header();
                    stats_string.push_str(&prom_rtt_mavg.render_sample(Some(&attributes), stats.rtt_mavg));
                }
                if let Ok(mut prometheus_data_receiver) = prometheus_data_receiver.lock() {
                    *prometheus_data_receiver = stats_string;
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
        if last_report == cur_time {
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
            Arg::with_name("probe_id")
                .short("i")
                .long("id")
                .takes_value(true)
                .default_value("default")
        )
        .arg(
            Arg::with_name("prometheus")
                .long("prometheus")
                .takes_value(true)
                .default_value("0.0.0.0:9151")
        )
        .get_matches();

    let config = lib::datatypes::ClientConfig {
        pps_limit: matches.value_of("pps-max").unwrap().parse().unwrap(),
        payload_size: matches.value_of("payload-size").unwrap().parse().unwrap(),
        use_sleep: matches.is_present("use-sleep"),
        balance: matches.value_of("balance").unwrap().parse().unwrap(),
        remote: matches.value_of("remote").unwrap().parse().unwrap(),
        key: matches.value_of("key").unwrap().to_string(),
        prometheus: matches.value_of("prometheus").unwrap().parse().unwrap(),
        probe_id: matches.value_of("probe_id").unwrap().to_string(),
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
