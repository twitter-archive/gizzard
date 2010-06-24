#!/usr/bin/env ruby
require 'vendor/thrift_client/simple'

module Gizzard
  module Thrift
    T = ThriftClient::Simple

    def self.struct(*args)
      T::StructType.new(*args)
    end

    ShardException = T.make_exception(:ShardException,
      T::Field.new(:description, T::STRING, 1)
    )

    ShardId = T.make_struct(:ShardId,
      T::Field.new(:hostname, T::STRING, 1),
      T::Field.new(:table_prefix, T::STRING, 2)
    )

    class ShardId
      def inspect
        "#{hostname}/#{table_prefix}"
      end
      
      alias_method :to_unix, :inspect
      
      def self.parse(string)
        new(*string.split("/"))
      end
    end

    ShardInfo = T.make_struct(:ShardInfo,
      T::Field.new(:id, struct(ShardId), 1),
      T::Field.new(:class_name, T::STRING, 2),
      T::Field.new(:source_type, T::STRING, 3),
      T::Field.new(:destination_type, T::STRING, 4),
      T::Field.new(:busy, T::I32, 5)
    )

    class ShardInfo
      def busy?
        busy && busy > 0
      end

      def inspect(short = false)
        "#{id.inspect}" + (busy? ? " (BUSY)" : "")
      end
      
      def to_unix
        [id.to_unix, class_name, busy? ? "busy" : "unbusy"].join("\t")
      end
    end

    LinkInfo = T.make_struct(:LinkInfo,
      T::Field.new(:up_id, struct(ShardId), 1),
      T::Field.new(:down_id, struct(ShardId), 2),
      T::Field.new(:weight, T::I32, 3)
    )

    class LinkInfo
      def inspect
        "#{up_id.inspect} -> #{down_id.inspect}" + (weight == 1 ? "" : " <#{weight}>")
      end
    end

    ShardMigration = T.make_struct(:ShardMigration,
      T::Field.new(:source_id, struct(ShardId), 1),
      T::Field.new(:destination_id, struct(ShardId), 2)
    )

    Forwarding = T.make_struct(:Forwarding,
      T::Field.new(:table_id, T::I32, 1),
      T::Field.new(:base_id, T::I64, 2),
      T::Field.new(:shard_id, struct(ShardId), 3)
    )

    class Forwarding
      #FIXME table_id is not human-readable
      def inspect
        "[#{table_id}] #{base_id.to_s(16)} -> #{shard_id.inspect}"
      end
    end

    class ShardManager < T::ThriftService
      thrift_method :create_shard, void, field(:shard, struct(ShardInfo), 1), :throws => exception(ShardException)
      thrift_method :delete_shard, void, field(:id, struct(ShardId), 1)
      thrift_method :get_shard, struct(ShardInfo), field(:id, struct(ShardId), 1)

      thrift_method :add_link, void, field(:up_id, struct(ShardId), 1), field(:down_id, struct(ShardId), 2), field(:weight, i32, 3)
      thrift_method :remove_link, void, field(:up_id, struct(ShardId), 1), field(:down_id, struct(ShardId), 2)

      thrift_method :list_upward_links, list(struct(LinkInfo)), field(:id, struct(ShardId), 1)
      thrift_method :list_downward_links, list(struct(LinkInfo)), field(:id, struct(ShardId), 1)

      thrift_method :get_child_shards_of_class, list(struct(ShardInfo)), field(:parent_id, struct(ShardId), 1), field(:class_name, string, 2)

      thrift_method :mark_shard_busy, void, field(:id, struct(ShardId), 1), field(:busy, i32, 2)
      thrift_method :copy_shard, void, field(:source_id, struct(ShardId), 1), field(:destination_id, struct(ShardId), 2)

      thrift_method :set_forwarding, void, field(:forwarding, struct(Forwarding), 1)
      thrift_method :replace_forwarding, void, field(:old_id, struct(ShardId), 1), field(:new_id, struct(ShardId), 2)

      thrift_method :get_forwarding, struct(Forwarding), field(:table_id, i32, 1), field(:base_id, i64, 2)
      thrift_method :get_forwarding_for_shard, struct(Forwarding), field(:shard_id, struct(ShardId), 1)

      thrift_method :get_forwardings, list(struct(Forwarding))
      thrift_method :reload_forwardings, void

      thrift_method :find_current_forwarding, struct(ShardInfo), field(:table_id, i32, 1), field(:id, i64, 2)

      thrift_method :shards_for_hostname, list(struct(ShardInfo)), field(:hostname, string, 1)
      thrift_method :get_busy_shards, list(struct(ShardInfo))

      thrift_method :rebuild_schema, void
    end
  end
end