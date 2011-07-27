package com.twitter.gizzard.fake

import com.twitter.gizzard.shards.RoutingNode

class ReadWriteShardAdapter(node: RoutingNode[Shard]) extends Shard {
  def get(k: String) = node.read.any(_.get(k))
  def put(k: String, v: String) = node.write.map(_.put(k, v)).head
}
