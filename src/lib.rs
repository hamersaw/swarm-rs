#[macro_use]
extern crate log;

pub mod prelude;
mod topology;
use topology::{Topology, TopologyBuilder};

use std::collections::HashMap;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct Node {
    id: u32,
    address: SocketAddr,
    metadata: HashMap<String, String>,
}

impl Node {
    pub fn new(id: u32, address: SocketAddr,
            metadata: HashMap<String, String>) -> Node {
        Node { id, address, metadata }
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }
}

pub struct Swarm<T: 'static + Topology + Sync + Send> {
    id: u32,
    join_handles: Vec<JoinHandle<()>>,
    nodes: Arc<RwLock<HashMap<u32, Node>>>,
    shutdown: Arc<AtomicBool>,
    topology: Arc<RwLock<T>>,
}

impl<T: 'static + Topology + Sync + Send> Swarm<T> {
    pub fn new(id: u32, address: SocketAddr,
            metadata: HashMap<String, String>,
            seed_address: Option<SocketAddr>,
            topology_builder: impl TopologyBuilder<T>)
            -> (Swarm<T>, Arc<RwLock<T>>) {
        // initialize nodes
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        {
            let mut nodes = nodes.write().unwrap();
            nodes.insert(id, Node::new(id, address, metadata));
        }

        // initialize topology
        let topology = Arc::new(RwLock::new(
                topology_builder.build(id, nodes.clone())
            ));

        // initialize swarm
        let swarm = Swarm {
            id,
            join_handles: Vec::new(),
            nodes,
            shutdown: Arc::new(AtomicBool::new(true)),
            topology: topology.clone(),
        };

        (swarm, topology)
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::{DhtBuilder, Swarm};
    use std::collections::HashMap;

    #[test]
    fn cycle_swarm() {
	// initialize topology builder
        let dht_builder = DhtBuilder::new(
            vec!(0, 6148914691236516864, 12297829382473033728));

	// initialize swarm
        let address = "127.0.0.1:12000".parse()
            .expect("parse seed addr");
        let seed_address = "127.0.0.1:12010".parse()
            .expect("parse seed addr");

        let mut metadata = HashMap::new();
        metadata.insert("rpc_addr".to_string(),
            "127.0.0.1:12002".to_string());
        metadata.insert("xfer_addr".to_string(),
            "127.0.0.1:12003".to_string());

        let (mut swarm, dht) = Swarm::new(0, address,
            metadata, Some(seed_address), dht_builder);

	// start swarm
	//swarm.start().expect("swarm start");

	{
	    let dht = dht.read().unwrap();
            match dht.get(0) {
                Some(node) => println!("{:?}",
                    node.get_metadata("rpc_addr")),
                None => println!("node not found"),
	    }
	}
        
        // stop swarm
        //swarm.stop().expect("swarm stop")
    }
}
