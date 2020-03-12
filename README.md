# swarm-rs
## OVERVIEW
A generalized distributed systems framework.

## TODO
- add logging
- parameterize gossip_interval_ms in 'Swarm'
- implement dht
    - locate(token: u64) -> Option<(u16, SocketAddr)>;
- implement single thread per connection server
- implement master / slave service
