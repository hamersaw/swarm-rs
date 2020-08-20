use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::Hasher;
use std::iter::Iterator;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr};

#[derive(Clone)]
pub struct Node {
    id: u32,
    address: SocketAddr,
    metadata: BTreeMap<String, String>,
}

impl Node {
    pub fn new(id: u32, address: SocketAddr) -> Node {
        Node { id, address, metadata: BTreeMap::new() }
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn get_address(&self) -> &SocketAddr {
        &self.address
    }

    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    pub fn read(reader: &mut impl Read)
            -> Result<Node, Box<dyn Error>> {
        // read id
        let id = reader.read_u32::<BigEndian>()?;

        // read address
        let ip_address = match reader.read_u8()? {
            4 => {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                IpAddr::from(buf)
            },
            6 => {
                let mut buf = [0u8; 16];
                reader.read_exact(&mut buf)?;
                IpAddr::from(buf)
            },
            _ => return Err("unknown ip version".into()),
        };
        let port = reader.read_u16::<BigEndian>()?;
        let address = SocketAddr::new(ip_address, port);

        let mut node = Node::new(id, address);

        // read metadata
        let metadata_len = reader.read_u16::<BigEndian>()?;
        for _ in 0..metadata_len {
            let key = read_string(reader)?;
            let value = read_string(reader)?;

            node.set_metadata(&key, &value);
        }

        Ok(Node::new(id, address))
    }

    pub fn set_metadata(&mut self, key: &str, value: &str) {
        self.metadata.insert(key.to_string(), value.to_string());
    }

    pub fn write(&self, writer: &mut impl Write)
            -> Result<(), Box<dyn Error>> {
        // write id
        writer.write_u32::<BigEndian>(self.id)?;

        // write address
        match self.address {
            SocketAddr::V4(socket_address_v4) => {
                writer.write_u8(4)?;
                writer.write(&socket_address_v4.ip().octets())?;
            },
            SocketAddr::V6(socket_address_v6) => {
                writer.write_u8(6)?;
                writer.write(&socket_address_v6.ip().octets())?;
            },
        }
        writer.write_u16::<BigEndian>(self.address.port())?;

        // write metadata
        writer.write_u16::<BigEndian>(self.metadata.len() as u16)?;
        for (key, value) in self.metadata.iter() {
            write_string(key, writer)?;
            write_string(value, writer)?;
        }

        Ok(())
    }
}

pub fn hash_nodes<'a>(nodes: impl Iterator<Item=&'a Node>) -> u64 {
    let mut hasher = DefaultHasher::new();
    for node in nodes {
        hasher.write_u32(node.get_id());
        for (key, value) in node.metadata.iter() {
            hasher.write(key.as_bytes());
            hasher.write(value.as_bytes());
        }
    }

    hasher.finish()
}

pub fn read_string(reader: &mut impl Read)
        -> Result<String, Box<dyn Error>> {
    let len = reader.read_u8()?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

pub fn write_string(value: &str, writer: &mut impl Write)
        -> Result<(), Box<dyn Error>> {
    writer.write_u8(value.len() as u8)?;
    writer.write(value.as_bytes())?;
    Ok(())
}
