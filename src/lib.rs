#[macro_use]
extern crate log;

pub mod prelude;
mod topology;
use topology::{Topology, TopologyBuilder};

use std::collections::HashMap;
use std::error::Error;
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
    address: SocketAddr,
    id: u32,
    join_handles: Vec<JoinHandle<()>>,
    nodes: Arc<RwLock<HashMap<u32, Node>>>,
    seed_address: Option<SocketAddr>,
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
            nodes.insert(id, Node::new(id, address));
        }

        // initialize topology
        let topology = Arc::new(RwLock::new(
                topology_builder.build(id, nodes.clone())
            ));

        // initialize swarm
        let swarm = Swarm {
            address, 
            id,
            join_handles: Vec::new(),
            nodes,
            seed_address,
            shutdown: Arc::new(AtomicBool::new(true)),
            topology: topology.clone(),
        };

        (swarm, topology)
    }

    pub fn set_metadata(&mut self, key: &str, value: &str) {
        let mut nodes = self.nodes.write().unwrap();
        let node = nodes.get_mut(&self.id).unwrap();
        node.set_metadata(key, value);
    }

    pub fn start(&mut self, thread_count: u8, thread_sleep_ms: u64,
            gossip_interval_ms: u64) -> Result<(), Box<dyn Error>> {
        // set shutdown false
        self.shutdown.store(false, Ordering::Relaxed);

        // start TcpListener 
        let listener = TcpListener::bind(self.address)?;

        // start gossip listening threads
        for _ in 0..thread_count {
            // clone gossip reply variables
            let listener_clone = listener.try_clone()?;
            listener_clone.set_nonblocking(true)?;
            let shutdown_clone = self.shutdown.clone();
            let thread_sleep = Duration::from_millis(thread_sleep_ms);
            let topology_clone = self.topology.clone();

            // start gossip reply threads
            let join_handle = thread::spawn(move || {
                if let Err(e) = gossip_listener(listener_clone,
                        shutdown_clone, thread_sleep, topology_clone) {
                    error!("gossip listener failed: {}", e);
                }
            });

            // capture gossip listener thread JoinHandle
            self.join_handles.push(join_handle);
        }

        // clone gossip request variables
        let gossip_interval = Duration::from_millis(gossip_interval_ms);
        let shutdown_clone = self.shutdown.clone();
        let topology_clone = self.topology.clone();

        // start gossip request thread
        let join_handle = thread::spawn(move || {
            if let Err(e) = gossiper(gossip_interval,
                    shutdown_clone, topology_clone) {
                error!("gossiper failed: {}", e);
            }
        });

        // capture gossip request thread JoinHandle
        self.join_handles.push(join_handle);

        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        // check if already shutdown
        if self.shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        // perform shutdown
        debug!("stopping swarm");
        self.shutdown.store(true, Ordering::Relaxed);

        // join threads
        while self.join_handles.len() != 0 {
            let join_handle = self.join_handles.pop().unwrap();
            if let Err(e) = join_handle.join() {
                warn!("join thread failure: {:?}", e);
            }
        }

        Ok(())
    }
}

fn gossip_listener<T: 'static + Topology + Sync + Send>(
        listener: TcpListener, shutdown: Arc<AtomicBool>,
        thread_sleep: Duration, topology: Arc<RwLock<T>>)
        -> Result<(), Box<dyn Error>> {
    for result in listener.incoming() {
        match result {
            Ok(mut stream) => {
                // send gossip reply
                let topology = topology.read().unwrap();
                if let Err(e) = topology.reply(&mut stream) {
                    warn!("gossip reply failure: {}", e);
                }

                // shutdown gossip connection
                if let Err(e) = stream.shutdown(Shutdown::Both) {
                    warn!("gossip shutdown failure: {}", e);
                }
            },
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // no connection available -> sleep
                thread::sleep(thread_sleep);
            },
            Err(ref e) if e.kind() !=
                    std::io::ErrorKind::WouldBlock => {
                // unknown error
                warn!("gossip connection failure: {}", e);
            },
            _ => {},
        }

        // check if shutdown
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
    }

    Ok(())
}

fn gossiper<T: 'static + Topology + Sync + Send>(
        gossip_interval: Duration, shutdown: Arc<AtomicBool>,
        topology: Arc<RwLock<T>>) -> Result<(), Box<dyn Error>> {
    let mut instant = Instant::now();
    instant -= gossip_interval;

    loop {
        // check if shutdown
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // sleep
        let elapsed = instant.elapsed();
        if elapsed < gossip_interval {
            thread::sleep(gossip_interval - elapsed);
        }

        // reset instance
        instant = Instant::now();

        // retrieve gossip address
        let topology = topology.read().unwrap();
        let socket_addr = match topology.gossip_addr() {
            Some(socket_addr) => socket_addr,
            None => continue,
        };

        // connect to SocketAddr
        let mut stream = match TcpStream::connect(&socket_addr) {
            Ok(stream) => stream,
            Err(e) => {
                warn!("gossip connection failure: {}", e);
                continue;
            },
        };

        // send gossip request
        if let Err(e) = topology.request(&mut stream) {
            warn!("gossip request failure: {}", e);
        }

        // shutdown gossip connection
        if let Err(e) = stream.shutdown(Shutdown::Both) {
            warn!("gossip shutdown failure: {}", e);
        }
    }

    Ok(())
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
	swarm.start(2, 50, 2000).expect("swarm start");

	{
	    let dht = dht.read().unwrap();
            match dht.get(0) {
                Some(node) => println!("{:?}",
                    node.get_metadata("rpc_addr")),
                None => println!("node not found"),
	    }
	}
        
        // stop swarm
        swarm.stop().expect("swarm stop")
    }
}
