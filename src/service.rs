use std::io::{self};
use std::collections::{BTreeMap, HashMap};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, RwLock};

pub trait SwarmService {
    fn gossip(&self, local_address: &SocketAddr) -> Result<(), io::Error>;

    fn process(&self, stream: &mut TcpStream) -> Result<(), io::Error>;
}

pub struct DhtService {
    local_id: u16,
    seed_address: Option<SocketAddr>,
    nodes: Arc<RwLock<HashMap<u16, SocketAddr>>>,
    tokens: Arc<RwLock<BTreeMap<u64, u16>>>,
}

impl DhtService {
    pub fn new(local_id: u16, seed_address: Option<SocketAddr>) -> DhtService {
        DhtService {
            local_id: local_id,
            seed_address: seed_address,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            tokens: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl SwarmService for DhtService {
    fn gossip(&self, _local_address: &SocketAddr) -> Result<(), io::Error> {
        // TODO - perform gossip
        unimplemented!();
    }

    fn process(&self, _stream: &mut TcpStream) -> Result<(), io::Error> {
        // TODO - process gossip request
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn cycle_dht_service() {
        // TODO - write request and read reply
        /*use std::net::{IpAddr, SocketAddr};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread::JoinHandle;
        use super::{DhtService, SwarmService};

        // initialize instance variables
        let ip_addr: IpAddr = "127.0.0.1".parse().expect("parse IpAddr");
        let socket_addr = SocketAddr::new(ip_addr, 15607);
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();

        // initialize DhtService
        let dht_service = DhtService::new(0, 1000, None);

        // start server
        dht_service.start(&socket_addr, shutdown.clone(),
            &mut join_handles).expect("service start");

        // stop server
        shutdown.store(true, Ordering::Relaxed);

        // join threads
        while join_handles.len() != 0 {
            let join_handle = join_handles.pop().expect("pop join handle");
            join_handle.join().expect("join handle join");
        }*/
    }
}
