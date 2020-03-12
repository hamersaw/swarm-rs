#[macro_use]
extern crate log;

mod server;
pub mod prelude;
use prelude::{SwarmServer, SwarmService};
mod service;

use std::io::{self};
use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
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
    pub fn bind<T: ToSocketAddrs>(addr: T) -> Result<Swarm, io::Error> {
        debug!("binding swarm");
        let mut addr_iter = addr.to_socket_addrs()?;
        let addr = match addr_iter.next() {
            Some(addr) => addr,
            None => return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable, "invalid bind address")),
        };

        trace!("addr: {}", addr);

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

    pub fn set_gossip_interval_ms(&mut self, gossip_interval_ms: u64) {
        self.gossip_interval_ms = gossip_interval_ms;
    }

    pub fn start<T: 'static + SwarmService + Send + Sync,
            U: SwarmServer>(&mut self, service: Arc<RwLock<T>>,
            server: U) -> Result<(), io::Error> {
        // start local swarm server
        debug!("starting swarm server");
        server.start(self.listener.try_clone()?, service.clone(),
            self.shutdown.clone(), &mut self.join_handles)?;

        {
            // initialize service
            let mut service = service.write().unwrap();
            service.initialize(&self.addr)?;
        }

        // start gossip thread for swarm service
        debug!("starting swarm service");
        let shutdown_clone = self.shutdown.clone();
        let gossip_interval_duration =
            Duration::from_millis(self.gossip_interval_ms);
        trace!("gossip_interval_ms: {}", self.gossip_interval_ms);
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
        debug!("stopping swarm");
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
        use std::sync::{Arc, RwLock};
        use crate::prelude::{DhtService, Swarm, ThreadPoolServer};
 
        // bind swarm to tcp socket
        let mut swarm = Swarm::bind("127.0.0.1:15605").expect("swarm bind");
        swarm.set_gossip_interval_ms(500);

        // start swarm
        let service = Arc::new(RwLock::new(DhtService::new(0, &[0], None)));
        let server = ThreadPoolServer::new(50, 4);
        swarm.start(service, server).expect("swarm start");

        // stop swarm
        swarm.stop().expect("swarm stop");
    }
}
