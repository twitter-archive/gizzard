namespace java com.twitter.gizzard.thrift
namespace rb Gizzard

exception ShardException {
  1: string description
}

struct ShardInfo {
  1: string class_name
  2: string table_prefix
  3: string hostname
  4: string source_type
  5: string destination_type
  6: i32 busy
  7: i32 shard_id
}

struct ChildInfo {
  1: i32 shard_id
  2: i32 position
  3: i32 weight
}

struct ShardMigration {
  1: i32 source_shard_id
  2: i32 destination_shard_id
  3: i32 replicating_shard_id
  4: i32 write_only_shard_id
}

struct Forwarding {
  1: list<i32> table_id
  2: i64 base_id
  3: i32 shard_id
}

service ShardManager {
  i32 create_shard(1: ShardInfo shard) throws(ShardException ex)
  i32 find_shard(1: ShardInfo shard) throws(ShardException ex)
  ShardInfo get_shard(1: i32 shard_id) throws(ShardException ex)
  void update_shard(1: ShardInfo shard) throws(ShardException ex)
  void delete_shard(1: i32 shard_id) throws(ShardException ex)

  void add_child_shard(1: i32 parent_shard_id, 2: i32 child_shard_id, 3: i32 position, 4: i32 weight) throws(ShardException ex)
  void remove_child_shard(1: i32 parent_shard_id, 2: i32 child_shard_id) throws(ShardException ex)
  void replace_child_shard(1: i32 old_child_shard_id, 2: i32 new_child_shard_id) throws(ShardException ex)
  list<ChildInfo> list_shard_children(1: i32 shard_id) throws(ShardException ex)

  void mark_shard_busy(1: i32 shard_id, 2: i32 busy) throws(ShardException ex)
  void copy_shard(1: i32 source_shard_id, 2: i32 destination_shard_id) throws(ShardException ex)
  ShardMigration setup_migration(1: ShardInfo source_shard_info, 2: ShardInfo destination_shard_info) throws(ShardException ex)
  void migrate_shard(1:ShardMigration migration) throws(ShardException ex)

  void set_forwarding(1: Forwarding forwarding) throws(ShardException ex)
  void replace_forwarding(1: i32 old_shard_id, 2: i32 new_shard_id) throws(ShardException ex)
  ShardInfo get_forwarding(1: list<i32> table_id, 2: i64 base_id) throws(ShardException ex)
  Forwarding get_forwarding_for_shard(1: i32 shard_id) throws(ShardException ex)
  list<Forwarding> get_forwardings() throws(ShardException ex)
  void reload_forwardings() throws(ShardException ex)
  ShardInfo find_current_forwarding(1: list<i32> table_id, 2: i64 id) throws(ShardException ex)

  list<i32> shard_ids_for_hostname(1: string hostname, 2: string class_name) throws(ShardException ex)
  list<ShardInfo> shards_for_hostname(1: string hostname, 2: string class_name) throws(ShardException ex)
  list<ShardInfo> get_busy_shards() throws(ShardException ex)
  ShardInfo get_parent_shard(1: i32 shard_id) throws(ShardException ex)
  ShardInfo get_root_shard(1: i32 shard_id) throws(ShardException ex)
  list<ShardInfo> get_child_shards_of_class(1: i32 parent_shard_id, 2: string class_name) throws(ShardException ex)
}
