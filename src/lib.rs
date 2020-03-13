#[macro_use]
extern crate log;

pub mod prelude;
use prelude::SwarmService;
pub mod service;

use std::io;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub struct Swarm<T: 'static + SwarmService + Sync + Send> {
    join_handles: Vec<JoinHandle<()>>,
    service: Arc<T>,
    shutdown: Arc<AtomicBool>,
    swarm_config: SwarmConfig,
}

impl<T: 'static + SwarmService + Sync + Send> Swarm<T> {
    pub fn new(service: T, swarm_config: SwarmConfig) -> Swarm<T> {
        Swarm {
            join_handles: Vec::new(),
            service: Arc::new(service),
            shutdown: Arc::new(AtomicBool::new(true)),
            swarm_config: swarm_config,
        }
    }

    pub fn start(&mut self) -> Result<(), io::Error> {
        // set shutdown false
        self.shutdown.store(false, Ordering::Relaxed);

        // start TcpListener 
        let listener = TcpListener::bind(self.swarm_config.addr)?;

        // start gossip listening threads
        for _ in 0..self.swarm_config.listener_threads {
            // clone gossip reply variables
            let listener_clone = listener.try_clone()?;
            listener_clone.set_nonblocking(true)?;
            let service_clone = self.service.clone();
            let sleep_duration = Duration::
                from_millis(self.swarm_config.listener_sleep_ms);
            let shutdown_clone = self.shutdown.clone();

            // start gossip reply threads
            let join_handle = thread::spawn(move || {
                for result in listener_clone.incoming() {
                    match result {
                        Ok(mut stream) => {
                            // send gossip reply
                            if let Err(e) = service_clone.reply(&mut stream) {
                                warn!("gossip reply failure: {}", e);
                            }

                            // shutdown gossip connection
                            if let Err(e) = stream.shutdown(Shutdown::Both) {
                                warn!("gossip shutdown failure: {}", e);
                            }
                        },
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            // no connection available -> sleep
                            thread::sleep(sleep_duration);
                        },
                        Err(ref e) if e.kind() !=
                                io::ErrorKind::WouldBlock => {
                            // unknown error
                            warn!("gossip connection failure: {}", e);
                        },
                        _ => {},
                    }

                    // check if shutdown
                    if shutdown_clone.load(Ordering::Relaxed) {
                        break;
                    }
                }
            });

            // capture gossip reply thread JoinHandle
            self.join_handles.push(join_handle);
        }

        // clone gossip request variables
        let shutdown_clone = self.shutdown.clone();
        let gossip_interval_duration =
            Duration::from_millis(self.swarm_config.gossip_interval_ms);
        let service_clone = self.service.clone();

        // start gossip request thread
        let join_handle = thread::spawn(move || {
            let mut instant = Instant::now();
            loop {
                // sleep
                let elapsed = instant.elapsed();
                if elapsed < gossip_interval_duration {
                    thread::sleep(gossip_interval_duration - elapsed);
                }
                
                // reset instance
                instant = Instant::now();

                // retrieve gossip address
                let socket_addr = match service_clone.gossip_addr() {
                    Some(socket_addr) => socket_addr,
                    None => continue,
                };

                // connect to SocketAddr
                let mut stream = match TcpStream::connect(&socket_addr) {
                    Ok(stream) => stream,
                    Err(e) => {
                        warn!("gossip connection failure: {}", e);
                        continue;
                    },
                };

                // send gossip request
                if let Err(e) = service_clone.request(&mut stream) {
                    warn!("gossip request failure: {}", e);
                }

                // shutdown gossip connection
                if let Err(e) = stream.shutdown(Shutdown::Both) {
                    warn!("gossip shutdown failure: {}", e);
                }

                // check if shutdown
                if shutdown_clone.load(Ordering::Relaxed) {
                    break;
                }
            }
        });

        // capture gossip request thread JoinHandle
        self.join_handles.push(join_handle);

        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), io::Error> {
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
            if let Err(e) = join_handle.join() {
                warn!("join thread failure: {:?}", e);
            }
        }

        Ok(())
    }
}

pub struct SwarmConfig {
    addr: SocketAddr,
    gossip_interval_ms: u64,
    listener_threads: u8,
    listener_sleep_ms: u64,
}

pub struct SwarmConfigBuilder {
    addr: SocketAddr,
    gossip_interval_ms: u64,
    listener_threads: u8,
    listener_sleep_ms: u64,
}

impl SwarmConfigBuilder {
    pub fn new() -> SwarmConfigBuilder {
        SwarmConfigBuilder {
            addr: "127.0.0.1:15600".parse().unwrap(),
            gossip_interval_ms: 1000,
            listener_threads: 2,
            listener_sleep_ms: 50,
        }
    }

    pub fn addr(mut self, addr: SocketAddr) -> SwarmConfigBuilder {
        self.addr = addr;
        self
    }

    pub fn gossip_interval_ms(mut self,
            gossip_interval_ms: u64) -> SwarmConfigBuilder {
        self.gossip_interval_ms = gossip_interval_ms;
        self
    }

    pub fn listener_threads(mut self,
            listener_threads: u8) -> SwarmConfigBuilder {
        self.listener_threads = listener_threads;
        self
    }

    pub fn listener_sleep_ms(mut self,
            listener_sleep_ms: u64) -> SwarmConfigBuilder {
        self.listener_sleep_ms = listener_sleep_ms;
        self
    }

    pub fn build(self) -> Result<SwarmConfig, io::Error> {
        Ok( SwarmConfig {
            addr: self.addr,
            gossip_interval_ms: self.gossip_interval_ms,
            listener_threads: self.listener_threads,
            listener_sleep_ms: self.listener_sleep_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::{DhtBuilder, Swarm, SwarmConfigBuilder};
    use std::net::SocketAddr;

    #[test]
    fn cycle_swarm() {
        let swarm_config = SwarmConfigBuilder::new()
            .addr("127.0.0.1:15600".parse().expect("parse SocketAddr"))
            .build().expect("build swarm config");

        let (mut swarm, dht) = DhtBuilder::new()
            .id(0)
            .swarm_config(swarm_config)
            .tokens(vec!(0, 6148914691236516864, 12297829382473033728))
            .build().expect("build dht");

        swarm.start().expect("swarm start");

        {
            let dht = dht.read().unwrap();
            let _ = dht.get(0);
        }

        swarm.stop();
    }
}
