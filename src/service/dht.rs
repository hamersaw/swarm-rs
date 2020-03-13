use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crate::prelude::{Swarm, SwarmConfig, SwarmConfigBuilder, SwarmService};

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::io::{self, Read, Write};
use std::sync::{Arc, RwLock};

pub struct Dht {
    nodes: BTreeMap<u16, SocketAddr>,
    tokens: BTreeMap<u64, u16>,
}

impl Dht {
    pub fn get(&self, id: u16) -> Option<&SocketAddr> {
        self.nodes.get(&id)
    }

    pub fn locate(&self, token: u64) -> Option<(&u16, &SocketAddr)> {
        // find smallest token that is larger than search token
        for (key, value) in self.tokens.iter() {
            if token < *key {
                let socket_addr = self.nodes.get(value).unwrap();
                return Some((value, socket_addr));
            }
        }

        // if there are tokens -> return lowest token
        if !self.tokens.is_empty() {
            let id = self.tokens.values().next().unwrap();
            let socket_addr = self.nodes.get(&id).unwrap();
            return Some((&id, socket_addr));
        }

        None
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
            for (id, addr) in dht.nodes.iter() {
                match (id, index) {
                    (x, _) if x == &self.id => {},
                    (_, 0) => return Some(addr.clone()),
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
            let local_addr = dht.get(self.id).unwrap();
            write_node(stream, self.id, &local_addr)?;

            // write node and token hashes
            stream.write_u64::<BigEndian>(hash_nodes(&dht.nodes))?;
            stream.write_u64::<BigEndian>(hash_tokens(&dht.tokens))?;
        }

        // process node updates
        let node_updates = stream.read_u16::<BigEndian>()?;
        for _ in 0..node_updates {
            let (id, socket_addr) = read_node(stream)?;

            let mut dht = self.dht.write().unwrap();
            if !dht.nodes.contains_key(&id) {
                debug!("registering node '{}' {}", id, socket_addr);
                dht.nodes.insert(id, socket_addr);
            }
        }

        // process token updates
        let token_updates = stream.read_u16::<BigEndian>()?;
        for _ in 0..token_updates {
            let token = stream.read_u64::<BigEndian>()?;
            let id = stream.read_u16::<BigEndian>()?;

            let mut dht = self.dht.write().unwrap();
            if !dht.tokens.contains_key(&token) {
                debug!("adding token {}:{}", token, id);
                dht.tokens.insert(token, id);
            }
        }

        Ok(())
    }

    fn reply(&self, stream: &mut TcpStream) -> Result<(), io::Error> {
        // read request node and node and token hashes
        let (id, socket_addr) = read_node(stream)?;
        let node_hash = stream.read_u64::<BigEndian>()?;
        let token_hash = stream.read_u64::<BigEndian>()?;

        {
            let dht = self.dht.read().unwrap();

            // write node updates
            if node_hash != hash_nodes(&dht.nodes) {
                stream.write_u16::<BigEndian>(dht.nodes.len() as u16)?;
                for (id, socket_addr) in dht.nodes.iter() {
                    write_node(stream, *id, socket_addr)?;
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
                debug!("registering node '{}' {}", id, socket_addr);
                dht.nodes.insert(id, socket_addr);
            }
        }

        Ok(())
    }
}

fn hash_nodes(nodes: &BTreeMap<u16, SocketAddr>) -> u64 {
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

fn read_node(stream: &mut TcpStream)
        -> Result<(u16, SocketAddr), io::Error> {
    let id = stream.read_u16::<BigEndian>()?;
    let ip_addr = match stream.read_u8()? {
        4 => {
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf)?;
            IpAddr::from(buf)
        },
        6 => {
            let mut buf = [0u8; 16];
            stream.read_exact(&mut buf)?;
            IpAddr::from(buf)
        },
        _ => return Err(io::Error::new(
            io::ErrorKind::InvalidData, "unknown ip version")),
    };
    let port = stream.read_u16::<BigEndian>()?;
    let socket_addr = SocketAddr::new(ip_addr, port);
    Ok((id, socket_addr))
}

fn write_node(stream: &mut TcpStream, id: u16,
        addr: &SocketAddr) -> Result<(), io::Error> {
    stream.write_u16::<BigEndian>(id)?;
    match addr {
        SocketAddr::V4(socket_addr_v4) => {
            stream.write_u8(4)?;
            stream.write(&socket_addr_v4.ip().octets())?;
        },
        SocketAddr::V6(socket_addr_v6) => {
            stream.write_u8(6)?;
            stream.write(&socket_addr_v6.ip().octets())?;
        },
    }
    stream.write_u16::<BigEndian>(addr.port())?;
    Ok(())
}

pub struct DhtBuilder {
    id: u16,
    seed_addr: Option<SocketAddr>,
    swarm_config: Option<SwarmConfig>,
    tokens: Vec<u64>,
}

impl DhtBuilder {
    pub fn new() -> DhtBuilder {
        DhtBuilder {
            id: 0,
            seed_addr: None,
            swarm_config: None,
            tokens: Vec::new(),
        }
    }

    pub fn id(mut self, id: u16) -> DhtBuilder {
        self.id = id;
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
 
    pub fn build(self) -> Result<(Swarm<DhtService>,
            Arc<RwLock<Dht>>), io::Error> {
        // if swarm_config not set -> build default
        let swarm_config = match self.swarm_config {
            Some(swarm_config) => swarm_config,
            None => SwarmConfigBuilder::new().build().unwrap(),
        };

        // initialize node and token tables
        let mut nodes = BTreeMap::new();
        nodes.insert(self.id, swarm_config.addr);

        let mut tokens = BTreeMap::new();
        for token in self.tokens {
            tokens.insert(token, self.id);
        }

        // initialize Dht
        let dht = Arc::new( RwLock::new( Dht {
            nodes: nodes,
            tokens: tokens,
        }));

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
