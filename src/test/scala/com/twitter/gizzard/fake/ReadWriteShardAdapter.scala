package com.twitter.gizzard
package fake

import com.twitter.gizzard.shards.RoutingNode

class ReadWriteShardAdapter(node: RoutingNode[Shard]) extends Shard {
  def get(k: String) = node.readOperation(_.get(k))
  def put(k: String, v: String) = node.writeOperation(_.put(k, v))
}
