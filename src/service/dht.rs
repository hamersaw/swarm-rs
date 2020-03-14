use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crate::prelude::{Swarm, SwarmConfig, SwarmConfigBuilder, SwarmService};
use crate::service::{read_node, write_node};

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::net::{SocketAddr, TcpStream};
use std::io;
use std::sync::{Arc, RwLock};

pub struct Dht {
    nodes: BTreeMap<u16, (SocketAddr, Option<SocketAddr>, Option<SocketAddr>)>,
    tokens: BTreeMap<u64, u16>,
}

impl Dht {
    pub fn add_token(&mut self, token: u64, id: u16) {
        self.tokens.insert(token, id);
        debug!("added token '{}' {}", token, id);
    }

    pub fn get(&self, id: u16) 
            -> Option<(&Option<SocketAddr>, &Option<SocketAddr>)> {
        match self.nodes.get(&id) {
            Some((_, rpc_addr, xfer_addr)) => Some((rpc_addr, xfer_addr)),
            None => None,
        }
    }

    pub fn locate(&self, token: u64) 
            -> Option<(&u16, (&Option<SocketAddr>, &Option<SocketAddr>))> {
        // find smallest token that is larger than search token
        for (key, value) in self.tokens.iter() {
            if token < *key {
                let (_, rpc_addr, xfer_addr) = self.nodes.get(value).unwrap();
                return Some((value, (rpc_addr, xfer_addr)));
            }
        }

        // if there are tokens -> return lowest token
        if !self.tokens.is_empty() {
            let id = self.tokens.values().next().unwrap();
            let (_, rpc_addr, xfer_addr) = self.nodes.get(&id).unwrap();
            return Some((&id, (rpc_addr, xfer_addr)));
        }

        None
    }

    pub fn register_node(&mut self, id: u16, swarm_addr: SocketAddr,
            rpc_addr: Option<SocketAddr>, xfer_addr: Option<SocketAddr>) {
        self.nodes.insert(id, (swarm_addr, rpc_addr, xfer_addr));
        debug!("registered node '{}' {} {:?} {:?}",
            id, swarm_addr, rpc_addr, xfer_addr);
    }
}

pub struct DhtService {
    id: u16,
    seed_addr: Option<SocketAddr>,
    dht: Arc<RwLock<Dht>>,
}

impl SwarmService for DhtService {
    fn gossip_addr(&self) -> Option<SocketAddr> {
        let dht = self.dht.read().unwrap();

        if dht.nodes.len() > 1 {
            // if more than local node is registered -> choose random
            let mut index = rand::random::<usize>() % (dht.nodes.len() - 1);
            for (id, addrs) in dht.nodes.iter() {
                match (id, index) {
                    (x, _) if x == &self.id => {},
                    (_, 0) => return Some(addrs.0.clone()),
                    _ => index -= 1,
                }
            }
        } else if let Some(seed_addr) = self.seed_addr {
            // if no other registered nodes -> return seed node
            return Some(seed_addr);
        }

        None
    }

    fn request(&self, stream: &mut TcpStream) -> Result<(), io::Error> {
        {
            let dht = self.dht.read().unwrap();

            // write local node information
            let (swarm_addr, rpc_addr, xfer_addr) =
                dht.nodes.get(&self.id).unwrap();
            write_node(stream, self.id, &swarm_addr, &rpc_addr, &xfer_addr)?;

            // write node and token hashes
            stream.write_u64::<BigEndian>(hash_nodes(&dht.nodes))?;
            stream.write_u64::<BigEndian>(hash_tokens(&dht.tokens))?;
        }

        // process node updates
        let node_updates = stream.read_u16::<BigEndian>()?;
        for _ in 0..node_updates {
            let (id, swarm_addr, rpc_addr, xfer_addr) = read_node(stream)?;

            let mut dht = self.dht.write().unwrap();
            if !dht.nodes.contains_key(&id) {
                dht.register_node(id, swarm_addr, rpc_addr, xfer_addr);
            }
        }

        // process token updates
        let token_updates = stream.read_u16::<BigEndian>()?;
        for _ in 0..token_updates {
            let token = stream.read_u64::<BigEndian>()?;
            let id = stream.read_u16::<BigEndian>()?;

            let mut dht = self.dht.write().unwrap();
            if !dht.tokens.contains_key(&token) {
                dht.add_token(token, id);
            }
        }

        Ok(())
    }

    fn reply(&self, stream: &mut TcpStream) -> Result<(), io::Error> {
        // read request node and node and token hashes
        let (id, swarm_addr, rpc_addr, xfer_addr) = read_node(stream)?;
        let node_hash = stream.read_u64::<BigEndian>()?;
        let token_hash = stream.read_u64::<BigEndian>()?;

        {
            let dht = self.dht.read().unwrap();

            // write node updates
            if node_hash != hash_nodes(&dht.nodes) {
                stream.write_u16::<BigEndian>(dht.nodes.len() as u16)?;
                for (id, (swarm_addr, rpc_addr, xfer_addr)) in dht.nodes.iter() {
                    write_node(stream, *id, swarm_addr, rpc_addr, xfer_addr)?;
                }
            } else {
                stream.write_u16::<BigEndian>(0)?;
            }

            // write token updates
            if token_hash != hash_tokens(&dht.tokens) {
                stream.write_u16::<BigEndian>(dht.tokens.len() as u16)?;
                for (token, id) in dht.tokens.iter() {
                    stream.write_u64::<BigEndian>(*token)?;
                    stream.write_u16::<BigEndian>(*id)?;
                }
            } else {
                stream.write_u16::<BigEndian>(0)?;
            }
        }
 
        {
            // add gossiping node to nodes if does not exist
            let mut dht = self.dht.write().unwrap();
            if !dht.nodes.contains_key(&id) {
                dht.register_node(id, swarm_addr, rpc_addr, xfer_addr);
            }
        }

        Ok(())
    }
}

fn hash_nodes(nodes: &BTreeMap<u16, (SocketAddr,
        Option<SocketAddr>, Option<SocketAddr>)>) -> u64 {
    let mut hasher = DefaultHasher::new();
    for key in nodes.keys() {
        hasher.write_u16(*key);
    }

    hasher.finish()
}

fn hash_tokens(tokens: &BTreeMap<u64, u16>) -> u64 {
    let mut hasher = DefaultHasher::new();
    for (token, id) in tokens.iter() {
        hasher.write_u64(*token);
        hasher.write_u16(*id);
    }

    hasher.finish()
}

pub struct DhtBuilder {
    id: u16,
    rpc_addr: Option<SocketAddr>,
    seed_addr: Option<SocketAddr>,
    swarm_config: Option<SwarmConfig>,
    tokens: Vec<u64>,
    xfer_addr: Option<SocketAddr>,
}

impl DhtBuilder {
    pub fn new() -> DhtBuilder {
        DhtBuilder {
            id: 0,
            rpc_addr: None,
            seed_addr: None,
            swarm_config: None,
            tokens: Vec::new(),
            xfer_addr: None,
        }
    }

    pub fn id(mut self, id: u16) -> DhtBuilder {
        self.id = id;
        self
    }

    pub fn rpc_addr(mut self, rpc_addr: SocketAddr) -> DhtBuilder {
        self.rpc_addr = Some(rpc_addr);
        self
    }

    pub fn seed_addr(mut self, seed_addr: SocketAddr) -> DhtBuilder {
        self.seed_addr = Some(seed_addr);
        self
    }

    pub fn swarm_config(mut self, swarm_config: SwarmConfig) -> DhtBuilder {
        self.swarm_config = Some(swarm_config);
        self
    }

    pub fn tokens(mut self, tokens: Vec<u64>) -> DhtBuilder {
        self.tokens = tokens;
        self
    }

    pub fn xfer_addr(mut self, xfer_addr: SocketAddr) -> DhtBuilder {
        self.xfer_addr = Some(xfer_addr);
        self
    }
 
    pub fn build(self) -> Result<(Swarm<DhtService>,
            Arc<RwLock<Dht>>), io::Error> {
        // if swarm_config not set -> build default
        let swarm_config = match self.swarm_config {
            Some(swarm_config) => swarm_config,
            None => SwarmConfigBuilder::new().build().unwrap(),
        };

        // initialize Dht
        let dht = Arc::new( RwLock::new( Dht {
            nodes: BTreeMap::new(),
            tokens: BTreeMap::new(),
        }));

        {
            let mut dht = dht.write().unwrap();

            // register local node
            dht.register_node(self.id, swarm_config.addr,
                self.rpc_addr, self.xfer_addr);

            // add local tokens
            for token in self.tokens {
                dht.add_token(token, self.id);
            }
        }

        // initialize DhtService
        let dht_service = DhtService {
            id: self.id,
            dht: dht.clone(),
            seed_addr: self.seed_addr,
        };

        // initailize Swarm
        let swarm = Swarm::<DhtService>::new(dht_service, swarm_config);

        Ok((swarm, dht))
    }
}
