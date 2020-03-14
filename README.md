# swarm-rs
## OVERVIEW
swarm-rs is a generalized distributed systems cluster communication framework. The goal of this framework is to quickly and easily initialize programs which may be setup in a variety of distributed systems topologies.

## EXAMPLE
    // build swarm config
    let swarm_config = SwarmConfigBuilder::new()
        .addr("127.0.0.1:12001".parse().expect("parse addr"))
        .build().expect("build swarm config");

    // build dht
    let (mut swarm, dht) = DhtBuilder::new()
        .id(opt.node_id)
        .seed_addr("127.0.0.1:12000".parse().expect("parse seed addr"))
        .swarm_config(swarm_config)
        .tokens(opt.tokens)
        .build()

    // start swarm
    swarm.start().expect("swarm start");

    {
        let dht = dht.read().unwrap();
        let _ = dht.get(0);
    }

    // stop swarm
    swarm.stop().expect("swarm stop")

## TODO
- dht - add app_port and xfer_port
- implement master / slave service
- testing?
