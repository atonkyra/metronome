extern crate serde;
extern crate serde_derive;
extern crate rmp_serde as rmps;
use std::net::SocketAddr;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct MetronomeMessage {
    pub mode: String,
    pub payload: Option<String>,
    pub seq: u64,
}

pub struct WrappedMessage {
    pub addr: SocketAddr,
    pub message: MetronomeMessage,
}

pub struct PingResult {
    pub timestamp: f64,
    pub deadline: f64,
}

pub struct Statistics {
    pub sent: u64,
    pub recv: u64,
    pub lost: u64,

    pub rtt_worst: f64,
    pub rtt_best: f64,
    pub rtt_mavg: f64,
}

pub struct ClientConfig {
    pub pps_limit: u64,
    pub payload_size: u64,
    pub balance: f64,
    pub remote: SocketAddr,
}

pub struct ServerConfig {
    pub bind: SocketAddr,
}

impl Statistics {
    pub fn new() -> Statistics {
        return Statistics {
            sent: 0,
            recv: 0,
            lost: 0,
            rtt_worst: 0.0,
            rtt_best: std::f64::INFINITY,
            rtt_mavg: std::f64::INFINITY,
        };
    }
}