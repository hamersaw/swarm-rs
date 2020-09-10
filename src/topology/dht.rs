use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::node::Node;
use crate::topology::{Topology, TopologyBuilder};

use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::Hasher;
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
            debug!("registering token [token={}, id={}]", token, id);
            tokens.insert(*token, id);
        }

        // initialize dht
        Dht { tokens: Arc::new(RwLock::new(tokens)), nodes }
    }
}

pub struct Dht {
    tokens: Arc<RwLock<BTreeMap<u64, u32>>>,
    nodes: Arc<RwLock<HashMap<u32, Node>>>,
}

impl Dht {
    pub fn locate(&self, token: u64) -> Option<Node> {
        let tokens = self.tokens.read().unwrap();

        // find smallest token that is larger than search token
        for (key, value) in tokens.iter() {
            if token < *key {
                let nodes = self.nodes.read().unwrap();
                return Some(nodes.get(value).unwrap().clone());
            }
        }

        // if there are tokens -> return lowest token
        if !tokens.is_empty() {
            let id = tokens.values().next().unwrap();
            
            let nodes = self.nodes.read().unwrap();
            return Some(nodes.get(&id).unwrap().clone());
        }

        None
    }

    pub fn nodes(&self) -> Vec<Node> {
        let nodes = self.nodes.read().unwrap();
        nodes.values().map(|x| x.clone()).collect()
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

    fn request(&self, id: u32, stream: &mut TcpStream)
            -> Result<(), Box<dyn Error>> {
        {
            let nodes = self.nodes.read().unwrap();
            let tokens = self.tokens.read().unwrap();

            // write local node
            let node = nodes.get(&id).unwrap();
            node.write(stream)?;

            // write node and token hashes
            stream.write_u64::<BigEndian>(
                crate::node::hash_nodes(nodes.values()))?;

            stream.write_u64::<BigEndian>(hash_tokens(&tokens))?;
        }

        // process node updates
        let node_updates = stream.read_u16::<BigEndian>()?;
        for _ in 0..node_updates {
            let node = Node::read(stream)?;

            let mut nodes = self.nodes.write().unwrap();
            if !nodes.contains_key(&node.get_id()) {
                debug!("registering node [id={}, address={}]",
                    node.get_id(), node.get_address());
            }
            nodes.insert(node.get_id(), node);
        }

        // process token updates
        let token_updates = stream.read_u16::<BigEndian>()?;
        for _ in 0..token_updates {
            let token = stream.read_u64::<BigEndian>()?;
            let id = stream.read_u32::<BigEndian>()?;

            let mut tokens = self.tokens.write().unwrap();
            if !tokens.contains_key(&token) {
                debug!("registering token [token={}, id={}]", token, id);
                tokens.insert(token, id);
            }
        }

        Ok(())
    }

    fn reply(&self, stream: &mut TcpStream)
            -> Result<(), Box<dyn Error>> {
        // read request node and node and token hashes
        let node = Node::read(stream)?;
        let node_hash = stream.read_u64::<BigEndian>()?;
        let token_hash = stream.read_u64::<BigEndian>()?;

        {
            // write node updates
            let nodes = self.nodes.read().unwrap();
            if node_hash != crate::node::hash_nodes(nodes.values()) {
                stream.write_u16::<BigEndian>(nodes.len() as u16)?;
                for node in nodes.values() {
                    node.write(stream)?;
                }
            } else {
                stream.write_u16::<BigEndian>(0)?;
            }
        }

        {
            // write token updates
            let tokens = self.tokens.read().unwrap();
            if token_hash != hash_tokens(&tokens) {
                stream.write_u16::<BigEndian>(tokens.len() as u16)?;
                for (token, id) in tokens.iter() {
                    stream.write_u64::<BigEndian>(*token)?;
                    stream.write_u32::<BigEndian>(*id)?;
                }
            } else {
                stream.write_u16::<BigEndian>(0)?;
            }
        }
 
        {
            // add gossiping node to nodes if does not exist
            let mut nodes = self.nodes.write().unwrap();
            if !nodes.contains_key(&node.get_id()) {
                debug!("registering node [id={}, address={}]",
                    node.get_id(), node.get_address());
            }
            nodes.insert(node.get_id(), node);
        }

        Ok(())
    }
}

fn hash_tokens(tokens: &BTreeMap<u64, u32>) -> u64 {
    let mut hasher = DefaultHasher::new();
    for (token, id) in tokens.iter() {
        hasher.write_u64(*token);
        hasher.write_u32(*id);
    }

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use crate::prelude::{DhtBuilder, Swarm};

    #[test]
    fn dht_get() {
	// initialize topology builder
        let dht_builder = DhtBuilder::new(
            vec!(0, 6148914691236516864, 12297829382473033728));

	// initialize swarm
        let address = "127.0.0.1:12000".parse()
            .expect("parse seed addr");
        let (_swarm, dht) =
            Swarm::new(0, address, None, dht_builder);

        let result = dht.get(15605);
        assert!(result.is_some());
        assert_eq!(result.unwrap().get_id(), 0);
    }
}
