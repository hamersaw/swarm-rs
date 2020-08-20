use crate::node::Node;

pub mod cluster;
pub mod dht;

use std::collections::HashMap;
use std::error::Error;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, RwLock};

pub trait TopologyBuilder<T: 'static + Topology + Sync + Send> {
    fn build(&self, id: u32,
        nodes: Arc<RwLock<HashMap<u32, Node>>>) -> T;
}

pub trait Topology {
    fn gossip_addr(&self, id: u32, seed_address: &Option<SocketAddr>)
        -> Option<SocketAddr>;
    fn request(&self, id: u32, stream: &mut TcpStream)
        -> Result<(), Box<dyn Error>>;
    fn reply(&self, stream: &mut TcpStream)
        -> Result<(), Box<dyn Error>>;
}
