pub mod dht;

use std::io;
use std::net::{SocketAddr, TcpStream};

pub trait SwarmService {
    fn addr(&self) -> Option<SocketAddr>;
    fn request(&self, stream: &mut TcpStream) -> Result<(), io::Error>;
    fn reply(&self, stream: &mut TcpStream) -> Result<(), io::Error>;
}
