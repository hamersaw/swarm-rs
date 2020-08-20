# swarm-rs
## OVERVIEW
swarm-rs is a generalized distributed systems cluster communication framework. The main goal of this effort is to quickly and easily setup a variety of distributed systems topologies.

## USAGE
#### SAMPLE DHT
	// initialize topology builder
    let dht_builder = DhtBuilder::new(
        vec!(0, 6148914691236516864, 12297829382473033728));

	// initialize swarm
    let address = "127.0.0.1:12000".parse()
        .expect("parse seed addr");
    let seed_address = "127.0.0.1:12010".parse()
        .expect("parse seed addr");
    let (mut swarm, dht) =
        Swarm::new(0, address, Some(seed_address), dht_builder);

    // set swarm instance metadata
    swarm.set_metadata("rpc_addr", "127.0.0.1:12002");
    swarm.set_metadata("xfer_addr", "127.0.0.1:12003");

	// start swarm
	swarm.start().expect("swarm start");

	{
	    let dht = dht.read().unwrap();
            match dht.get(0) {
                Some(node) => println!("{:?}",
                    node.get_metadata("rpc_addr")),
                None => println!("node not found"),
	    }
	}

	// stop swarm
	swarm.stop().expect("swarm start");

## TODO
- handle node metadata in Dht
- improve logging - info when starting everything
- implement master / slave topology
- implement mock topology
