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

        // start gossip server
        for _ in 0..self.swarm_config.listener_threads {
            // clone variables
            let listener_clone = listener.try_clone()?;
            listener_clone.set_nonblocking(true)?;
            let service_clone = self.service.clone();
            let sleep_duration = Duration::from_millis(50); // TODO - parameterize
            let shutdown_clone = self.shutdown.clone();

            // spawn new thread to listen for tcp connections
            let join_handle = thread::spawn(move || {
                for result in listener_clone.incoming() {
                    match result {
                        Ok(mut stream) => {
                            // handle connection
                            match service_clone.reply(&mut stream) {
                                Err(ref e) if e.kind() != std::io
                                        ::ErrorKind::UnexpectedEof => {
                                    error!("failed to process stream {}", e);
                                },
                                _ => {},
                            }
                            /*let mut service = service_clone.write().unwrap();
                            match service.reply(&mut stream) {
                                Err(ref e) if e.kind() != std::io
                                        ::ErrorKind::UnexpectedEof => {
                                    error!("failed to process stream {}", e);
                                },
                                _ => {},
                            }*/
                            stream.shutdown(Shutdown::Both).expect("stream shutdown");
                        },
                        Err(ref e) if e.kind() ==
                                std::io::ErrorKind::WouldBlock => {
                            // no connection available -> sleep
                            thread::sleep(sleep_duration);
                        },
                        Err(ref e) if e.kind() !=
                                std::io::ErrorKind::WouldBlock => {
                            // unknown error
                            error!("failed to connect client: {}", e);
                        },
                        _ => {},
                    }

                    // check if shutdown
                    if shutdown_clone.load(Ordering::Relaxed) {
                        break;
                    }
                }
            });

            self.join_handles.push(join_handle);
        }

        let shutdown_clone = self.shutdown.clone();
        let gossip_interval_duration =
            Duration::from_millis(self.swarm_config.gossip_interval_ms);
        let service_clone = self.service.clone();
        let join_handle = thread::spawn(move || {
            loop {
                let instant = Instant::now();

                let socket_addr = service_clone.addr();
                //trace!("GOSSIP ADDR: {:?}", socket_addr);
                if let Some(socket_addr) = socket_addr {
                //if let Some(socket_addr) = service_clone.addr() {
                    // connect to gossip node
                    let mut stream = match TcpStream::connect(&socket_addr) {
                        Ok(stream) => stream,
                        Err(e) => {
                            warn!("gossip connection failed: {}", socket_addr);
                            continue;
                        }
                    };

                    // send request
                    if let Err(e) = service_clone.request(&mut stream) {
                        error!("gossip request: {}", e);
                    }

                    stream.shutdown(Shutdown::Both).expect("stream shutdown");
                }

                /*// retrieve gossip address
                let gossip_addr = {
                    let service = service_clone.read().unwrap();
                    service.addr()
                };

                if let Some(socket_addr) = gossip_addr {
                    // connect to gossip node
                    let mut stream = match TcpStream::connect(&socket_addr) {
                        Ok(stream) => stream,
                        Err(e) => {
                            warn!("gossip connection failed: {}", socket_addr);
                            continue;
                        }
                    };

                    // send request
                    let mut service = service_clone.write().unwrap();
                    if let Err(e) = service.request(&mut stream) {
                        error!("gossip request: {}", e);
                    }
                }*/

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

    pub fn stop(&mut self) -> Result<(), io::Error> {
        Ok(())
    }
}

pub struct SwarmConfig {
    addr: SocketAddr,
    gossip_interval_ms: u64,
    listener_threads: u8,
}

pub struct SwarmConfigBuilder {
    addr: SocketAddr,
    gossip_interval_ms: u64,
    listener_threads: u8,
}

impl SwarmConfigBuilder {
    pub fn new() -> SwarmConfigBuilder {
        SwarmConfigBuilder {
            addr: "127.0.0.1:15600".parse().unwrap(),
            gossip_interval_ms: 500,
            listener_threads: 2,
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

    pub fn build(self) -> Result<SwarmConfig, io::Error> {
        Ok( SwarmConfig {
            addr: self.addr,
            gossip_interval_ms: self.gossip_interval_ms,
            listener_threads: self.listener_threads,
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
