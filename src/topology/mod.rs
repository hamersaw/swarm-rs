use crate::Node;

pub mod dht;

use std::collections::HashMap;
use std::error::Error;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, RwLock};

pub trait TopologyBuilder<T: 'static + Topology + Sync + Send> {
    fn build(&self, id: u32, nodes: Arc<RwLock<HashMap<u32, Node>>>)
        -> T;
}

pub trait Topology {
    fn gossip_addr(&self) -> Option<SocketAddr>;
    fn request(&self, stream: &mut TcpStream)
        -> Result<(), Box<dyn Error>>;
    fn reply(&self, stream: &mut TcpStream)
        -> Result<(), Box<dyn Error>>;
}
