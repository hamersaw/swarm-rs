use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use std::io::{self, Read, Write};
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::sync::{Arc, RwLock};

pub trait SwarmService {
    fn gossip(&self, local_address: &SocketAddr) -> Result<(), io::Error>;

    fn process(&self, stream: &mut TcpStream) -> Result<(), io::Error>;
}

pub struct DhtService {
    local_id: u16,
    seed_addr: Option<SocketAddr>,
    nodes: Arc<RwLock<HashMap<u16, SocketAddr>>>,
    tokens: Arc<RwLock<BTreeMap<u64, u16>>>,
}

impl DhtService {
    pub fn new(local_id: u16, seed_addr: Option<SocketAddr>) -> DhtService {
        DhtService {
            local_id: local_id,
            seed_addr: seed_addr,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            tokens: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl SwarmService for DhtService {
    fn gossip(&self, local_addr: &SocketAddr) -> Result<(), io::Error> {
        // choose random node
        let mut socket_addr: Option<SocketAddr> = None;
        {
            let nodes = self.nodes.read().unwrap();

            // check if only local node is registered
            if nodes.len() > 1 {
                // choose random node
                let mut index = rand::random::<usize>() % (nodes.len() - 2);
                for (id, addr) in nodes.iter() {
                    match (id, index) {
                        (x, _) if x == &self.local_id => {},
                        (_, 0) => socket_addr = Some(addr.clone()),
                        _ => index -= 1,
                    }
                }
            } else if let Some(seed_addr) = self.seed_addr {
                socket_addr = Some(seed_addr);
            }
        }

        // check if gossip node exists
        let socket_addr = match socket_addr {
            Some(socket_addr) => socket_addr,
            None => return Ok(()),
        };

        // connect to gossip node
        let mut stream = TcpStream::connect(&socket_addr)?;

        // send request - node_id, socket_addr, nodes_hash, tokens_hash
        write_node(&mut stream, self.local_id, local_addr)?;

        // TODO - send nodes hash
        // TODO - send tokens hash

        unimplemented!();
    }

    fn process(&self, stream: &mut TcpStream) -> Result<(), io::Error> {
        // read request - node_id, socket_addr, nodes_hash, tokens_hash
        let (id, socket_addr) = read_node(stream)?;

        println!("{} - {}", id, socket_addr);

        // TODO - read nodes hash
        // TODO - read tokens hash

        // TODO - write gossip response
 
        unimplemented!();
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
        use super::{DhtService, SwarmService};

        // initialize listening service
        let ip_addr: IpAddr = "127.0.0.1".parse().expect("parse IpAddr");
        let socket_addr_1 = SocketAddr::new(ip_addr, 15607);
        let service = DhtService::new(0, None);
 
        // open TcpListener
        let listener = TcpListener::bind(&socket_addr_1)
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
        let socket_addr_2 = SocketAddr::new(ip_addr, 15608);
        let dht_service = DhtService::new(1, Some(socket_addr_1));

        // send gossip request
        dht_service.gossip(&socket_addr_2).expect("gossip");
    }
}
