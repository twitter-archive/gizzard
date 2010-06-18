module Gizzard
  module Recipes
    def setup_migration_shards(source, destination)
      raise "Cannot migrate to the same shard" if source == destination

      write_only_shard_id = shard_id("localhost", source.table_prefix + "_migrate_write_only")
      write_only_shard = shard_info(write_only_shard_id, Consts::WRITE_ONLY_SHARD, "", "", 0)
      create_shard(write_only_shard)
      add_link(write_only_shard_id, destination)

      replicating_shard_id = shard_id("localhost", source.table_prefix + "_migrate_replicating")
      replicating_shard = shard_info(replicating_shard_id, Consts::REPLICATING_SHARD, "", "", 0)
      create_shard(replicating_shard)

      list_upward_links(source).each do |link|
        add_link(link.up_id, replicating_shard_id, link.weight)
        remove_link(link.up_id, link.down_id)
      end

      add_link(replicating_shard_id, source, 1)
      add_link(replicating_shard_id, write_only_shard_id, 1)
      replace_forwarding(source, replicating_shard_id)

      puts "Replication structure set up: #{source} => #{destination}."
    end

    def finish_migration(source, destination)
      replicating_shard_id = shard_id("localhost", source.table_prefix + "_migrate_replicating")
      write_only_shard_id = shard_id("localhost", source.table_prefix + "migrate_write_only")
      list_upward_links(replicating_shard_id).each do |link|
        add_link(link.up_id, destination, link.weight)
        remove_link(link.up_id, replicating_shard_id)
      end

      replace_forwarding(replicating_shard_id, destination)
      delete_shard(replicating_shard_id)
      delete_shard(write_only_shard_id)
      delete_shard(source_id)
    end
  end

  class Manager
    include DSL
    include Recipes

    def initialize(hostname, port = 7917)
      @client = hostname.is_a?(Gizzard::Thrift::ShardManager) ? hostname : Gizzard::Thrift::ShardManager.new(hostname, port)
    end

    def method_missing(method,  *args, &block)
      @client.send(method, *args, &block)
    end

    def inspect_shard_tree(root_shard, prefix = '')
    end
  end
end