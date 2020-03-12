# swarm-rs
## OVERVIEW
A generalized distributed systems framework.

## TODO
- add logging
- finish dht service functionality
    - nodes() -> Iter<(u16, SocketAddr)>
    - lookup(token: u64) -> Option<(u16, SocketAddr)>
    - get(id: u16) -> Option<SocketAddr>
    - tokens() -> Iter<(u64, u16)>
- implement single thread per connection server
- implement master / slave service
