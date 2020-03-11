use crate::service::SwarmService;

use std::io;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub trait SwarmServer {
    fn start<T: 'static + SwarmService + Send + Sync>(&self,
        listener: TcpListener, service: Arc<T>, shutdown: Arc<AtomicBool>,
        join_handles: &mut Vec<JoinHandle<()>>) -> Result<(), io::Error>;
}

pub struct ThreadPoolServer {
    sleep_ms: u64,
    thread_count: u8,
}

impl ThreadPoolServer {
    pub fn new(sleep_ms: u64, thread_count: u8) -> ThreadPoolServer {
        ThreadPoolServer {
            sleep_ms: sleep_ms,
            thread_count: thread_count,
        }
    }
}

impl SwarmServer for ThreadPoolServer {
    fn start<T: 'static + SwarmService + Send + Sync>(&self,
            listener: TcpListener, service: Arc<T>, shutdown: Arc<AtomicBool>,
            join_handles: &mut Vec<JoinHandle<()>>)  -> Result<(), io::Error> {
        for _ in 0..self.thread_count {
            // clone variables
            let listener_clone = listener.try_clone()?;
            listener_clone.set_nonblocking(true)?;
            let sleep_duration = Duration::from_millis(self.sleep_ms);
            let shutdown_clone = shutdown.clone();
            let service_clone = service.clone();

            // spawn new thread to listen for tpc connections
            let join_handle = thread::spawn(move || {
                for result in listener_clone.incoming() {
                    match result {
                        Ok(mut stream) => {
                            // handle connection
                            match service_clone.process(&mut stream) {
                                Err(ref e) if e.kind() != std::io
                                        ::ErrorKind::UnexpectedEof => {
                                    error!("failed to process stream {}", e);
                                },
                                _ => {},
                            }
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

            join_handles.push(join_handle);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::net::{SocketAddr, TcpStream};
    
    use crate::service::SwarmService;

    struct NullService {
    }

    impl SwarmService for NullService {
        fn gossip(&self, _: &SocketAddr) -> Result<(), io::Error> {
            Ok(())
        }

        fn process(&self, _: &mut TcpStream) -> Result<(), io::Error> {
            Ok(())
        }
    }

    #[test]
    fn cycle_thread_pool_server() {
        use std::net::TcpListener;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread::JoinHandle;
        use super::{SwarmServer, ThreadPoolServer};

        // open TcpListener
        let listener = TcpListener::bind("127.0.0.1:15606")
            .expect("TcpListener bind");

        // initialize instance variables
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();

        // initialize ThreadPoolServer
        let thread_pool_server = ThreadPoolServer::new(4, 50);

        // initialize NullService
        let service = Arc::new(NullService{});

        // start server
        thread_pool_server.start(listener, service, shutdown.clone(),
            &mut join_handles).expect("server start");

        // stop server
        shutdown.store(true, Ordering::Relaxed);

        // join threads
        while join_handles.len() != 0 {
            let join_handle = join_handles.pop().expect("pop join handle");
            join_handle.join().expect("join handle join");
        }
    }
}
