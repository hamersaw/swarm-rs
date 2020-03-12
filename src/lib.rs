#[macro_use]
extern crate log;

mod server;
pub mod prelude;
use prelude::{SwarmServer, SwarmService};
mod service;

use std::io::{self};
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub struct Swarm {
    addr: SocketAddr,
    join_handles: Vec<JoinHandle<()>>,
    gossip_interval_ms: u64,
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
}

impl Swarm {
    pub fn bind(addr: SocketAddr) -> Result<Swarm, io::Error> {
        Ok ( 
            Swarm {
                addr: addr,
                join_handles: Vec::new(),
                gossip_interval_ms: 2000,
                listener: TcpListener::bind(addr)?,
                shutdown: Arc::new(AtomicBool::new(true)),
            }
        )
    }

    pub fn start<T: 'static + SwarmService + Send + Sync,
            U: SwarmServer>(&mut self, service: Arc<RwLock<T>>,
            server: U) -> Result<(), io::Error> {
        // start local swarm server
        server.start(self.listener.try_clone()?, service.clone(),
            self.shutdown.clone(), &mut self.join_handles)?;

        {
            // initialize service
            let mut service = service.write().unwrap();
            service.initialize(&self.addr)?;
        }

        // start gossip thread for swarm service
        let shutdown_clone = self.shutdown.clone();
        let gossip_interval_duration =
            Duration::from_millis(self.gossip_interval_ms);
        let join_handle = thread::spawn(move || {
            loop {
                let instant = Instant::now();

                // gossip call
                {
                    let mut service = service.write().unwrap();
                    if let Err(e) = service.gossip() {
                        error!("gossip: {}", e);
                    }
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
        use std::sync::{Arc, RwLock};
        use crate::prelude::{DhtService, Swarm, ThreadPoolServer};
 
        // initialize swarm server and service
        let service = Arc::new(RwLock::new(DhtService::new(0, &[0], None)));
        let server = ThreadPoolServer::new(4, 50);

        // bind swarm to tcp socket
        let ip_addr: IpAddr = "127.0.0.1".parse().expect("parse IpAddr");
        let socket_addr = SocketAddr::new(ip_addr, 15605);
        let mut swarm = Swarm::bind(socket_addr).expect("swarm bind");

        // start swarm
        swarm.start(service, server).expect("swarm start");

        // stop swarm
        swarm.stop().expect("swarm stop");
    }
}
