# swarm-rs
## OVERVIEW
swarm-rs is a generalized distributed systems cluster communication framework. The main goal of this effort is to quickly and easily setup a variety of distributed systems topologies.

## EXAMPLE
    // build swarm config
    let swarm_config = SwarmConfigBuilder::new()
        .addr("127.0.0.1:12010".parse().expect("parse swarm addr"))
        .build().expect("build swarm config");

    // build dht
    let (mut swarm, dht) = DhtBuilder::new()
        .id(0)
        .rpc_addr("127.0.0.1:12011".parse().expect("parse rpc addr"))
        .seed_addr("127.0.0.1:12000".parse().expect("parse seed addr"))
        .swarm_config(swarm_config)
        .tokens(vec!(0, 6148914691236516864, 12297829382473033728))
        .xfer_addr("127.0.0.1:12012".parse().expect("parse xfer addr"))
        .build()

    // start swarm
    swarm.start().expect("swarm start");

    {
        let dht = dht.read().unwrap();
        match dht.get(0) {
            Some((rpc_addr, xfer_addr)) =>
                println!("{:?} {:?}", rpc_addr, xfer_addr),
            None => println!("node not found"),
        }
    }

    // stop swarm
    swarm.stop().expect("swarm stop")

## TODO
- implement master / slave service
