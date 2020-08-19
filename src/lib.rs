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
    pub fn new(id: u32, address: SocketAddr) -> Node {
        Node { id, address, metadata: HashMap::new() }
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    pub fn set_metadata(&mut self, key: &str, value: &str) {
        self.metadata.insert(key.to_string(), value.to_string());
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
            seed_address: Option<SocketAddr>,
            topology_builder: impl TopologyBuilder<T>)
            -> (Swarm<T>, Arc<RwLock<T>>) {
        // initialize nodes
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        {
            let mut nodes = nodes.write().unwrap();

            let mut node = Node::new(id, address);
            nodes.insert(id, node);
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

    pub fn set_metadata(&mut self, key: &str, value: &str) {
        let mut nodes = self.nodes.write().unwrap();
        let mut node = nodes.get_mut(&self.id).unwrap();
        node.set_metadata(key, value);
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::{DhtBuilder, Swarm};

    #[test]
    fn cycle_swarm() {
	// initialize topology builder
        let dht_builder = DhtBuilder::new(
            vec!(0, 6148914691236516864, 12297829382473033728));

	// initialize swarm
        let address = "127.0.0.1:12000".parse()
            .expect("parse seed addr");
        let (mut swarm, dht) =
            Swarm::new(0, address, None, dht_builder);

        // set swarm instance metadata
        swarm.set_metadata("rpc_addr", "127.0.0.1:12002");
        swarm.set_metadata("xfer_addr", "127.0.0.1:12003");

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
