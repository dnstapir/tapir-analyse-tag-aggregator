# Configuration example 
This in an example config for a `dns_tunnel` aggregator
```toml
debug = true
tags = ["tag1", "tag2"]     # which tags to watch
observation = "dns_tunnel"  # must exist in lib/common/observations.go
allow_custom = true         # explicit flag for watching tags that are not well-defined
#dry_run = true             # explicit flag for skipping to set observations

[nats]
debug = true
url = "nats://nats:4222"
event_subject = "core-integration-test.events.new_qname"
observation_subject_prefix = "core-integration-test.observations"
tag_subject_prefix = "core-integration-test.tags"

[[nats.observation_buckets]]    # bucket to set observation in
observation = "dns_tunnel"
name = "dns_tunnel_bucket"
create = false
ttl = 10

[nats.tag_bucket]               # bucket to read tags from
name = "tag_bucket"

[api]
active = false
```
