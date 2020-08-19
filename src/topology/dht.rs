use crate::Node;
use crate::topology::{Topology, TopologyBuilder};

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, RwLock};

pub struct DhtBuilder {
    tokens: Vec<u64>,
}

impl DhtBuilder {
    pub fn new(tokens: Vec<u64>) -> DhtBuilder {
        DhtBuilder { tokens }
    }
}

impl TopologyBuilder<Dht> for DhtBuilder {
    fn build(&self, id: u32,
            nodes: Arc<RwLock<HashMap<u32, Node>>>) -> Dht {
        // initialize tokens
        let mut tokens = BTreeMap::new();
        for token in self.tokens.iter() {
            tokens.insert(*token, id);
        }

        // initialize dht
        Dht { tokens, nodes }
    }
}

pub struct Dht {
    tokens: BTreeMap<u64, u32>,
    nodes: Arc<RwLock<HashMap<u32, Node>>>,
}

impl Dht {
    pub fn get(&self, token: u64) -> Option<Node> {
        // find smallest token that is larger than search token
        for (key, value) in self.tokens.iter() {
            if token < *key {
                let nodes = self.nodes.read().unwrap();
                return Some(nodes.get(value).unwrap().clone());
            }
        }

        // if there are tokens -> return lowest token
        if !self.tokens.is_empty() {
            let id = self.tokens.values().next().unwrap();
            
            let nodes = self.nodes.read().unwrap();
            return Some(nodes.get(&id).unwrap().clone());
        }

        None
    }
}

impl Topology for Dht {
    fn gossip_addr(&self, id: u32, seed_address: &Option<SocketAddr>)
            -> Option<SocketAddr> {
        let nodes = self.nodes.read().unwrap();
        if nodes.len() > 1 {
            // if more than local node is registered -> choose random
            let mut index = rand::random::<usize>() % (nodes.len() - 1);
            for (node_id, node) in nodes.iter() {
                match (node_id, index) {
                    (x, _) if x == &id => {},
                    (_, 0) => return Some(node.get_address().clone()),
                    _ => index -= 1,
                }
            }
        } else if let Some(seed_address) = seed_address {
            // if no other registered nodes -> return seed node
            return Some(seed_address.clone());
        }

        None
    }

    fn request(&self, stream: &mut TcpStream)
            -> Result<(), Box<dyn Error>> {
        unimplemented!();
    }

    fn reply(&self, stream: &mut TcpStream)
            -> Result<(), Box<dyn Error>> {
        unimplemented!();
    }
}
