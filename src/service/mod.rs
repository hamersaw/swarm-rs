pub mod dht;

use std::io;
use std::net::{SocketAddr, TcpStream};

pub trait SwarmService {
    fn gossip(&self, local_address: &SocketAddr) -> Result<(), io::Error>;

    fn process(&self, stream: &mut TcpStream) -> Result<(), io::Error>;
}
