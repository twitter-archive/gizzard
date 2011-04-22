package com.twitter.gizzard
package shards


class BlockedShardFactory[T] extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    new BlockedShard(shardInfo, weight, children)
  }
}

class BlockedShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends PassThroughNode[T](shardInfo, weight, children) {

  private def exception = new ShardRejectedOperationException("shard is offline", shardInfo.id)

  override def readAllOperation[A](method: T => A) = Seq(Left(exception))
  override def readOperation[A](method: T => A)    = throw exception
  override def writeOperation[A](method: T => A)   = throw exception

  override def rebuildableReadOperation[A](method: T => Option[A])(rebuild: (T, T) => Unit) = throw exception
}
