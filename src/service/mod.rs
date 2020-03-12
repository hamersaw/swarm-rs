pub mod dht;

use std::io;
use std::net::{SocketAddr, TcpStream};

pub trait SwarmService {
    fn gossip(&mut self) -> Result<(), io::Error>;
    fn initialize(&mut self, local_addr: &SocketAddr) -> Result<(), io::Error>;
    fn process(&mut self, stream: &mut TcpStream) -> Result<(), io::Error>;
}
