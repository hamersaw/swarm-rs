use crate::service::SwarmService;

pub mod threadpool;

use std::io;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicBool;
use std::thread::JoinHandle;

pub trait SwarmServer {
    fn start<T: 'static + SwarmService + Send + Sync>(&self,
        listener: TcpListener, service: Arc<RwLock<T>>,
        shutdown: Arc<AtomicBool>, join_handles: &mut Vec<JoinHandle<()>>) 
        -> Result<(), io::Error>;
}
