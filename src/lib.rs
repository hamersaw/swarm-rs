#[macro_use]
extern crate log;

mod server;
pub use server::{SwarmServer, ThreadPoolServer};
mod service;
pub use service::{DhtService, SwarmService};

use std::io::{self};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub struct Swarm {
    address: SocketAddr,
    join_handles: Vec<JoinHandle<()>>,
    gossip_interval_ms: u64,
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
}

impl Swarm {
    pub fn bind(address: SocketAddr) -> Result<Swarm, io::Error> {
        Ok ( 
            Swarm {
                address: address,
                join_handles: Vec::new(),
                gossip_interval_ms: 2000, // TODO - parameterize
                listener: TcpListener::bind(address)?,
                shutdown: Arc::new(AtomicBool::new(true)),
            }
        )
    }

    pub fn start<T: 'static + SwarmService + Send + Sync,
            U: SwarmServer>(&mut self, service: Arc<T>,
            server: U) -> Result<(), io::Error> {
        // start local swarm server
        server.start(self.listener.try_clone()?, service.clone(),
            self.shutdown.clone(), &mut self.join_handles)?;

        // start gossip thread for swarm service
        let address_clone = self.address.clone();
        let shutdown_clone = self.shutdown.clone();
        let gossip_interval_duration =
            Duration::from_millis(self.gossip_interval_ms);
        let join_handle = thread::spawn(move || {
            loop {
                let instant = Instant::now();

                // gossip call
                if let Err(e) = service.gossip(&address_clone) {
                    error!("gossip: {}", e);
                }

                // check if shutdown
                if shutdown_clone.load(Ordering::Relaxed) {
                    break;
                }

                // sleep
                let elapsed = instant.elapsed();
                if elapsed < gossip_interval_duration {
                    thread::sleep(gossip_interval_duration - elapsed);
                }
            }
        });

        self.join_handles.push(join_handle);
        Ok(())
    }

    pub fn stop(mut self) -> thread::Result<()> {
        // check if already shutdown
        if self.shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        // perform shutdown
        self.shutdown.store(true, Ordering::Relaxed);

        // join threads
        while self.join_handles.len() != 0 {
            let join_handle = self.join_handles.pop().unwrap();
            join_handle.join()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn cycle_swarm() {
        use std::net::{IpAddr, SocketAddr};
        use std::sync::Arc;
        use super::{DhtService, Swarm, ThreadPoolServer};
 
        // bind swarm to tcp socket
        let ip_addr: IpAddr = "127.0.0.1".parse().expect("parse IpAddr");
        let socket_addr = SocketAddr::new(ip_addr, 15605);
        let mut swarm = Swarm::bind(socket_addr).expect("swarm bind");

        // start swarm
        let dht_service = Arc::new(DhtService::new(0, None));
        let thread_pool_server = ThreadPoolServer::new(4, 50);
        swarm.start(dht_service, thread_pool_server).expect("swarm start");

        // stop swarm
        swarm.stop().expect("swarm stop");
    }
}
