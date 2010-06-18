module Gizzard
  module DSL
    [:shard_id, :shard_info, :link_info, :shard_migration, :forwarding].each do |method|
      struct_class = method.to_s.capitalize.gsub(/_(.)/) { $1.upcase }
      module_eval "def #{method}(*args); Thrift::#{struct_class}.new(*args) end"
    end

    def shard(hostname, table_prefix, *rest)
      id = shard_id(hostname, table_prefix)
      shard_info(id, *rest)
    end
  end
end
