namespace java com.twitter.gizzard.thrift
namespace rb Gizzard

exception ShardException {
  1: string description
}

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

service ShardManager {
  void create_shard(1: ShardInfo shard) throws(1: ShardException ex)
  void delete_shard(1: ShardId id) throws(1: ShardException ex)
  ShardInfo get_shard(1: ShardId id) throws(1: ShardException ex)

  void add_link(1: ShardId up_id, 2: ShardId down_id, 3: i32 weight) throws(1: ShardException ex)
  void remove_link(1: ShardId up_id, 2: ShardId down_id) throws(1: ShardException ex)

  list<LinkInfo> list_upward_links(1: ShardId id) throws(1: ShardException ex)
  list<LinkInfo> list_downward_links(1: ShardId id) throws(1: ShardException ex)

  list<LinkInfo> list_all_links() throws(1: ShardException ex)
  list<ShardInfo> list_all_shards() throws(1: ShardException ex)
  list<Forwarding> list_all_forwardings() throws(1: ShardException ex)

  list<ShardInfo> get_child_shards_of_class(1: ShardId parent_id, 2: string class_name) throws(1: ShardException ex)

  void mark_shard_busy(1: ShardId id, 2: i32 busy) throws(1: ShardException ex)
  void copy_shard(1: ShardId source_id, 2: ShardId destination_id) throws(1: ShardException ex)

  void set_forwarding(1: Forwarding forwarding) throws(1: ShardException ex)
  void replace_forwarding(1: ShardId old_id, 2: ShardId new_id) throws(1: ShardException ex)
  Forwarding get_forwarding(1: i32 table_id, 2: i64 base_id) throws(1: ShardException ex)
  Forwarding get_forwarding_for_shard(1: ShardId id) throws(1: ShardException ex)
  list<Forwarding> get_forwardings() throws(1: ShardException ex)
  void reload_forwardings() throws(1: ShardException ex)

  void remove_forwarding(1: Forwarding forwarding) throws(1: ShardException ex)

  list<ShardInfo> shards_for_hostname(1: string hostname) throws(1: ShardException ex)
  list<string> list_hostnames() throws(1: ShardException ex)
  list<ShardInfo> get_busy_shards() throws(1: ShardException ex)

  void rebuild_schema() throws(1: ShardException ex)

  // operate on the current forwardings in memory
  ShardInfo find_current_forwarding(1: i32 table_id, 2: i64 id) throws(1: ShardException ex)
}
