package com.twitter.gizzard
package shards


class ReadOnlyShardFactory[T] extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    new ReadOnlyShard(shardInfo, weight, children)
  }
}

class ReadOnlyShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends PassThroughNode[T](shardInfo, weight, children) {

  override def writeOperation[A](method: T => A) = {
    throw new ShardRejectedOperationException("shard is read-only", shardInfo.id)
  }
}
