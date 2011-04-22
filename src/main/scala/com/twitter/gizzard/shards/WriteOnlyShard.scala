package com.twitter.gizzard
package shards


class WriteOnlyShardFactory[T] extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    new WriteOnlyShard(shardInfo, weight, children)
  }
}

class WriteOnlyShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends PassThroughNode[T](shardInfo, weight, children) {

  private def exception = new ShardRejectedOperationException("shard is write-only", shardInfo.id)

  override def readAllOperation[A](method: T => A) = Seq(Left(exception))
  override def readOperation[A](method: T => A)    = throw exception
  override def rebuildableReadOperation[A](method: T => Option[A])(rebuild: (T, T) => Unit) = throw exception
}
