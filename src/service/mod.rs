use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

pub mod dht;

use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};

pub trait SwarmService {
    fn gossip_addr(&self) -> Option<SocketAddr>;
    fn request(&self, stream: &mut TcpStream) -> Result<(), io::Error>;
    fn reply(&self, stream: &mut TcpStream) -> Result<(), io::Error>;
}

fn read_node(stream: &mut TcpStream) -> Result<(u16, SocketAddr,
        Option<SocketAddr>, Option<SocketAddr>), io::Error> {
    let id = stream.read_u16::<BigEndian>()?;
    let swarm_addr = read_addr(stream)?;
    let rpc_addr = match stream.read_u8()? {
        1 => Some(read_addr(stream)?),
        0 => None,
        _ => return Err(io::Error::new(
            io::ErrorKind::InvalidData, "unknown ip version")),
    };
    let xfer_addr = match stream.read_u8()? {
        1 => Some(read_addr(stream)?),
        0 => None,
        _ => return Err(io::Error::new(
            io::ErrorKind::InvalidData, "unknown ip version")),
    };
    Ok((id, swarm_addr, rpc_addr, xfer_addr))
}

fn read_addr(stream: &mut TcpStream) -> Result<SocketAddr, io::Error> {
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
    Ok(SocketAddr::new(ip_addr, port))
}

fn write_node(stream: &mut TcpStream, id: u16,
        swarm_addr: &SocketAddr, rpc_addr: &Option<SocketAddr>,
        xfer_addr: &Option<SocketAddr>) -> Result<(), io::Error> {
    stream.write_u16::<BigEndian>(id)?;
    write_addr(stream, swarm_addr)?;
    match rpc_addr {
        Some(rpc_addr) => {
            stream.write_u8(1)?;
            write_addr(stream, rpc_addr)?;
        },
        None => stream.write_u8(0)?,
    }
    match xfer_addr {
        Some(xfer_addr) => {
            stream.write_u8(1)?;
            write_addr(stream, xfer_addr)?;
        },
        None => stream.write_u8(0)?,
    }
    Ok(())
}

fn write_addr(stream: &mut TcpStream, addr: &SocketAddr)
        -> Result<(), io::Error> {
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
