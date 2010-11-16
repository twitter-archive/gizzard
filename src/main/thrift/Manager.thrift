namespace java com.twitter.gizzard.thrift
namespace rb Gizzard

exception GizzardException {
  1: string description
}

// Shard Tree Structs

struct ShardId {
  1: string hostname
  2: string table_prefix
}

struct ShardInfo {
  1: ShardId id
  2: string class_name
  3: string source_type
  4: string destination_type
  5: i32 busy
}

struct LinkInfo {
  1: ShardId up_id
  2: ShardId down_id
  3: i32 weight
}

struct Forwarding {
  1: i32 table_id
  2: i64 base_id
  3: ShardId shard_id
}


// Remote Host Structs

enum HostStatus {
  Normal  = 0
  Offline = 1
  Blocked = 2
}

struct Host {
  1: string hostname
  2: i32 port
  3: string cluster
  4: HostStatus status
}

service Manager {
  void reload_config() throws(1: GizzardException ex)
  void rebuild_schema() throws(1: GizzardException ex)

  // operate on the current forwardings in memory
  ShardInfo find_current_forwarding(1: i32 table_id, 2: i64 id) throws(1: GizzardException ex)


  // shard tree management

  void create_shard(1: ShardInfo shard) throws(1: GizzardException ex)
  void delete_shard(1: ShardId id) throws(1: GizzardException ex)

  void add_link(1: ShardId up_id, 2: ShardId down_id, 3: i32 weight) throws(1: GizzardException ex)
  void remove_link(1: ShardId up_id, 2: ShardId down_id) throws(1: GizzardException ex)

  void set_forwarding(1: Forwarding forwarding) throws(1: GizzardException ex)
  void replace_forwarding(1: ShardId old_id, 2: ShardId new_id) throws(1: GizzardException ex)
  void remove_forwarding(1: Forwarding forwarding) throws(1: GizzardException ex)

  ShardInfo get_shard(1: ShardId id) throws(1: GizzardException ex)
  list<ShardInfo> shards_for_hostname(1: string hostname) throws(1: GizzardException ex)
  list<ShardInfo> get_busy_shards() throws(1: GizzardException ex)

  list<LinkInfo> list_upward_links(1: ShardId id) throws(1: GizzardException ex)
  list<LinkInfo> list_downward_links(1: ShardId id) throws(1: GizzardException ex)

  Forwarding get_forwarding(1: i32 table_id, 2: i64 base_id) throws(1: GizzardException ex)
  Forwarding get_forwarding_for_shard(1: ShardId id) throws(1: GizzardException ex)
  list<Forwarding> get_forwardings() throws(1: GizzardException ex)

  list<string> list_hostnames() throws(1: GizzardException ex)

  void mark_shard_busy(1: ShardId id, 2: i32 busy) throws(1: GizzardException ex)
  void copy_shard(1: ShardId source_id, 2: ShardId destination_id) throws(1: GizzardException ex)


  // job scheduler management

  void retry_errors() throws(1: GizzardException ex)
  void stop_writes() throws(1: GizzardException ex)
  void resume_writes() throws(1: GizzardException ex)

  void retry_errors_for(1: i32 priority) throws(1: GizzardException ex)
  void stop_writes_for(1: i32 priority) throws(1: GizzardException ex)
  void resume_writes_for(1: i32 priority) throws(1: GizzardException ex)

  bool is_writing(1: i32 priority) throws(1: GizzardException ex)


  // remote host cluster management

  void add_remote_host(1: Host host) throws(1: GizzardException ex)
  void remove_remote_host(1: string hostname, 2: i32 port) throws (1: GizzardException ex)
  void set_remote_host_status(1: string hostname, 2: i32 port, 3: HostStatus status) throws (1: GizzardException ex)
  void set_remote_cluster_status(1: string cluster, 2: HostStatus status) throws (1: GizzardException ex)

  Host get_remote_host(1: string hostname, 2: i32 port) throws (1: GizzardException ex)
  list<string> list_remote_clusters() throws (1: GizzardException ex)
  list<Host> list_remote_hosts() throws (1: GizzardException ex)
  list<Host> list_remote_hosts_in_cluster(1: string cluster) throws (1: GizzardException ex)
}
