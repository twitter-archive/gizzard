#!/bin/bash
cd `dirname $0`
function g {
  echo "g $@" >&2
  ../../../script/gizzmo -Cconfig.yaml "$@" 2>&1
}
function expect {
  diff - "expected/$1" && echo "    success." || echo "    failed." && exit 1
}

g help info | expect help-info.txt



# include Gizzard::Thrift
# 20.times do |i|
#   repl = service.create_shard(ShardInfo.new(repl_id = ShardId.new("localhost", "table_repl_#{i}"), "com.twitter.service.flock.edges.ReplicatingShard", "", "", 0))
#   a    = service.create_shard(ShardInfo.new(a_id    = ShardId.new("localhost", "table_a_#{i}"), "com.twitter.service.flock.edges.SqlShard", "INT UNSIGNED", "INT UNSIGNED", 0))
#   b    = service.create_shard(ShardInfo.new(b_id    = ShardId.new("localhost", "table_b_#{i}"), "com.twitter.service.flock.edges.SqlShard", "INT UNSIGNED", "INT UNSIGNED", 0))
#
#   service.add_link(repl_id, a_id, 2)
#   service.add_link(repl_id, b_id, 1)
#
#   service.set_forwarding(Forwarding.new(0, i * 1000, repl_id))
# end