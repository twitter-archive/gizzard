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

struct HostWeightInfo {
  1: string hostname
  2: double weight_write
  3: double weight_read
}

struct Forwarding {
  1: i32 table_id
  2: i64 base_id
  3: ShardId shard_id
}

struct NameServerState {
  1: list<ShardInfo> shards
  2: list<LinkInfo> links
  3: list<Forwarding> forwardings
  4: i32 table_id
  5: optional list<HostWeightInfo> hostWeights = {}
}


// Remote Host Structs

enum HostStatus {
  Normal     = 0
  Blackholed = 1
  Blocked    = 2
}

struct Host {
  1: string hostname
  2: i32 port
  3: string cluster
  4: HostStatus status
}

struct AddLinkRequest {
  1: required ShardId up_id
  2: required ShardId down_id
  3: required i32 weight
}

struct RemoveLinkRequest {
  1: required ShardId up_id
  2: required ShardId down_id
}

# a 'commit' object is really just a placeholder, so we represent it here as 1 byte
typedef bool Commit

union TransformOperation {
  1: optional ShardInfo create_shard
  2: optional ShardId delete_shard
  3: optional AddLinkRequest add_link
  4: optional RemoveLinkRequest remove_link
  5: optional Forwarding set_forwarding
  6: optional Forwarding remove_forwarding
  7: optional Commit commit
}

struct LogEntry {
  1: required i32 id
  2: required TransformOperation command
}

service Manager {
  void reload_updated_forwardings() throws(1: GizzardException ex)
  void reload_config() throws(1: GizzardException ex)

  // operate on the current forwardings in memory
  ShardInfo find_current_forwarding(1: i32 table_id, 2: i64 id) throws(1: GizzardException ex)

  // shard tree management

  void create_shard(1: ShardInfo shard) throws(1: GizzardException ex)
  void delete_shard(1: ShardId id) throws(1: GizzardException ex)

  void add_link(1: ShardId up_id, 2: ShardId down_id, 3: i32 weight) throws(1: GizzardException ex)
  void remove_link(1: ShardId up_id, 2: ShardId down_id) throws(1: GizzardException ex)

  void set_host_weight(1: HostWeightInfo hw) throws(1: GizzardException ex)
  list<HostWeightInfo> list_host_weights() throws(1: GizzardException ex)

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
  void copy_shard(1: list<ShardId> ids) throws(1: GizzardException ex)
  void repair_shard(1: list<ShardId> ids) throws(1: GizzardException ex)
  void diff_shards(1: list<ShardId> ids) throws(1: GizzardException ex)

  list<i32> list_tables() throws(1: GizzardException ex)

  list<NameServerState> dump_nameserver(1: list<i32> table_id) throws(1: GizzardException ex)

  void batch_execute(1: list<TransformOperation> commands) throws (1: GizzardException ex)

  // job scheduler management

  void retry_errors() throws(1: GizzardException ex)
  void stop_writes() throws(1: GizzardException ex)
  void resume_writes() throws(1: GizzardException ex)

  void retry_errors_for(1: i32 priority) throws(1: GizzardException ex)
  void stop_writes_for(1: i32 priority) throws(1: GizzardException ex)
  void resume_writes_for(1: i32 priority) throws(1: GizzardException ex)

  bool is_writing(1: i32 priority) throws(1: GizzardException ex)
  i32 queue_size(1: i32 priority) throws(1: GizzardException ex)
  i32 error_queue_size(1: i32 priority) throws(1: GizzardException ex)

  void add_fanout(1: string suffix) throws(1: GizzardException ex)
  void remove_fanout(1: string suffix) throws(1: GizzardException ex)
  list<string> list_fanout() throws(1: GizzardException ex)

  void add_fanout_for(1: i32 priority, 2: string suffix) throws(1: GizzardException ex)
  void remove_fanout_for(1: i32 priority, 2: string suffix) throws(1: GizzardException ex)

  // rollback log management

  // create a new log for the given name (must not already exist), and return a log_id
  binary log_create(1: string log_name)
  // return the log_id for the given log_name, which must exist
  binary log_get(1: string log_name)
  // push the given command log entry to the end of the given log
  void log_entry_push(1: binary log_id, 2: LogEntry log_entry)
  // peek at (but don't remove) the last entry in the log
  list<LogEntry> log_entry_peek(1: binary log_id, 2: i32 count)
  // pop (remove) the last entry in the log, which must match the given id
  void log_entry_pop(1: binary log_id, 2: i32 log_entry_id)

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
