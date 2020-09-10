use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::node::Node;
use crate::topology::{Topology, TopologyBuilder};

use std::collections::HashMap;
use std::error::Error;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, RwLock};

pub struct ClusterBuilder {
}

impl ClusterBuilder {
    pub fn new() -> ClusterBuilder {
        ClusterBuilder { }
    }
}

impl TopologyBuilder<Cluster> for ClusterBuilder {
    fn build(&self, _id: u32,
            nodes: Arc<RwLock<HashMap<u32, Node>>>) -> Cluster {
        Cluster { nodes }
    }
}

pub struct Cluster {
    nodes: Arc<RwLock<HashMap<u32, Node>>>,
}

impl Topology for Cluster {
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

            // write local node
            let node = nodes.get(&id).unwrap();
            node.write(stream)?;

            // write node and token hashes
            stream.write_u64::<BigEndian>(
                crate::node::hash_nodes(nodes.values()))?;
        }

        // process node updates
        let node_updates = stream.read_u16::<BigEndian>()?;
        for _ in 0..node_updates {
            let node = Node::read(stream)?;

            let mut nodes = self.nodes.write().unwrap();
            if !nodes.contains_key(&node.get_id()) {
                debug!("registering node {} {}:{}", node.get_id(),
                    node.get_ip_address(), node.get_port());
            }
            nodes.insert(node.get_id(), node);
        }

        Ok(())
    }

    fn reply(&self, stream: &mut TcpStream)
            -> Result<(), Box<dyn Error>> {
        // read request node and node and token hashes
        let node = Node::read(stream)?;
        let node_hash = stream.read_u64::<BigEndian>()?;

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
            // add gossiping node to nodes if does not exist
            let mut nodes = self.nodes.write().unwrap();
            if !nodes.contains_key(&node.get_id()) {
                debug!("registering node {} {}:{}", node.get_id(),
                    node.get_ip_address(), node.get_port());
            }
            nodes.insert(node.get_id(), node);
        }

        Ok(())
    }
}
