# swarm-rs
## OVERVIEW
A generalized distributed systems framework.

## EXAMPLE
    use std::sync::{Arc, RwLock};
    use crate::prelude::{DhtService, Swarm, ThreadPoolServer};

    // bind swarm to tcp socket
    let mut swarm = Swarm::bind("127.0.0.1:15605").expect("swarm bind");
    swarm.set_gossip_interval_ms(500);

    // start swarm
    let service = Arc::new(RwLock::new(DhtService::new(0, &[0], None)));
    let server = ThreadPoolServer::new(4, 50);
    swarm.start(service, server).expect("swarm start");

    // perform operations on dht
    {
        let service = service.read().unwrap();
        match service.locate(1023523) {
            Some((id, socket_addr)) => println!("addr: {}", socket_addr),
            None => panic!("dht node not found"),
        }
    }

    // stop swarm
    swarm.stop().expect("swarm stop");

## TODO
- implement single thread per connection server
- implement master / slave service
