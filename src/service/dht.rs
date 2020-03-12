use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::prelude::SwarmService;

use std::io::{self, Read, Write};
use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::net::{IpAddr, SocketAddr, TcpStream};

pub struct DhtService {
    local_id: u16,
    seed_addr: Option<SocketAddr>,
    nodes: BTreeMap<u16, SocketAddr>,
    tokens: BTreeMap<u64, u16>,
}

impl DhtService {
    pub fn new(local_id: u16, tokens: &[u64],
            seed_addr: Option<SocketAddr>) -> DhtService {
        let mut map = BTreeMap::new();
        for token in tokens {
            map.insert(*token, local_id);
        }

        DhtService {
            local_id: local_id,
            seed_addr: seed_addr,
            nodes: BTreeMap::new(),
            tokens: map,
        }
    }

    pub fn get(&self, id: u16) -> Option<&SocketAddr> {
        self.nodes.get(&id)
    }

    pub fn locate(&self, token: u64) -> Option<(&u16, &SocketAddr)> {
        unimplemented!();
    }

    pub fn nodes(&self) -> &BTreeMap<u16, SocketAddr> {
        &self.nodes
    }

    pub fn tokens(&self) -> &BTreeMap<u64, u16> {
        &self.tokens
    }

    fn hash_nodes(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        for key in self.nodes.keys() {
            hasher.write_u16(*key);
        }

        hasher.finish()
    }

    fn hash_tokens(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        for (token, id) in self.tokens.iter() {
            hasher.write_u64(*token);
            hasher.write_u16(*id);
        }

        hasher.finish()
    }
}

impl SwarmService for DhtService {
    fn gossip(&mut self) -> Result<(), io::Error> {
        // choose random node
        let mut socket_addr: Option<SocketAddr> = None;
 
        // check if only local node is registered
        if self.nodes.len() > 1 {
            // choose random node
            let mut index = rand::random::<usize>() % (self.nodes.len() - 2);
            for (id, addr) in self.nodes.iter() {
                match (id, index) {
                    (x, _) if x == &self.local_id => {},
                    (_, 0) => socket_addr = Some(addr.clone()),
                    _ => index -= 1,
                }
            }
        } else if let Some(seed_addr) = self.seed_addr {
            socket_addr = Some(seed_addr);
        }

        // check if gossip node exists
        let socket_addr = match socket_addr {
            Some(socket_addr) => socket_addr,
            None => return Ok(()),
        };

        // connect to gossip node
        let mut stream = TcpStream::connect(&socket_addr)?;

        // send request - node_id, socket_addr, nodes_hash, tokens_hash
        let local_addr = self.get(self.local_id).unwrap();
        write_node(&mut stream, self.local_id, &local_addr)?;

        // process node updates
        stream.write_u64::<BigEndian>(self.hash_nodes())?;

        let node_updates = stream.read_u16::<BigEndian>()?;
        for _ in 0..node_updates {
            let (id, socket_addr) = read_node(&mut stream)?;
            if !self.nodes.contains_key(&id) {
                self.nodes.insert(id, socket_addr);
            }
        }

        // process token updates
        stream.write_u64::<BigEndian>(self.hash_tokens())?;

        let token_updates = stream.read_u16::<BigEndian>()?;
        for _ in 0..token_updates {
            let token = stream.read_u64::<BigEndian>()?;
            let id = stream.read_u16::<BigEndian>()?;
            if !self.tokens.contains_key(&token) {
                self.tokens.insert(token, id);
            }
        }

        Ok(())
    }

    fn initialize(&mut self, local_addr: &SocketAddr)
            -> Result<(), io::Error> {
        self.nodes.insert(self.local_id, local_addr.clone());
        Ok(())
    }

    fn process(&mut self, stream: &mut TcpStream) -> Result<(), io::Error> {
        // read request - node_id, socket_addr, nodes_hash, tokens_hash
        let (id, socket_addr) = read_node(stream)?;

        // write node updates
        let node_hash = stream.read_u64::<BigEndian>()?;
        if node_hash != self.hash_nodes() {
            stream.write_u16::<BigEndian>(self.nodes.len() as u16)?;
            for (id, socket_addr) in self.nodes.iter() {
                write_node(stream, *id, socket_addr)?;
            }
        } else {
            stream.write_u16::<BigEndian>(0)?;
        }

        // write token updates
        let token_hash = stream.read_u64::<BigEndian>()?;
        if token_hash != self.hash_tokens() {
            stream.write_u16::<BigEndian>(self.tokens.len() as u16)?;
            for (token, id) in self.tokens.iter() {
                stream.write_u64::<BigEndian>(*token)?;
                stream.write_u16::<BigEndian>(*id)?;
            }
        } else {
            stream.write_u16::<BigEndian>(0)?;
        }
 
        {
            // add gossiping node to nodes if does not exist
            if !self.nodes.contains_key(&id) {
                self.nodes.insert(id, socket_addr);
            }
        }

        Ok(())
    }
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

#[cfg(test)]
mod tests {
    #[test]
    fn dht_service_gossip() {
        use std::net::{IpAddr, SocketAddr, TcpListener};
        use std::thread;
        use crate::prelude::{DhtService, SwarmService};

        // initialize listening service
        let ip_addr: IpAddr = "127.0.0.1".parse().expect("parse IpAddr");
        let socket_addr = SocketAddr::new(ip_addr, 15607);
        let mut service = DhtService::new(0, &[0, 10], None);
        service.initialize(&socket_addr).expect("service initialize");
 
        // open TcpListener
        let listener = TcpListener::bind(&socket_addr)
            .expect("TcpListener bind");

        // start listening gossiper
        let _join_handle = thread::spawn(move || {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    // handle connection
                    match service.process(&mut stream) {
                        Err(ref e) if e.kind() != std::io
                                ::ErrorKind::UnexpectedEof => {
                            panic!("failed to process stream {}", e);
                        },
                        _ => {},
                    }
                },
                Err(e) => panic!("failed to connect client: {}", e),
            }
        });

        // initialize gossiper
        let socket_addr2 = SocketAddr::new(ip_addr, 15608);
        let mut service2 = DhtService::new(1, &[5, 15], Some(socket_addr));
        service2.initialize(&socket_addr2).expect("service2 initialize");

        // send gossip request
        service2.gossip().expect("gossip");
    }
}
