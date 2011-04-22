package com.twitter.gizzard
package shards


class BlackHoleShardFactory[T] extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    new BlackHoleShard(shardInfo, weight, children)
  }
}

/**
 * A shard that refuses all read/write traffic in a silent way.
 */
class BlackHoleShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends PassThroughNode[T](shardInfo, weight, children) {

  private def exception = throw new ShardBlackHoleException(shardInfo.id)

  override def readAllOperation[A](method: T => A) = Seq[Either[Throwable,A]]()
  override def readOperation[A](method: T => A)    = throw exception
  override def writeOperation[A](method: T => A)   = throw exception

  override def rebuildableReadOperation[A](method: T => Option[A])(rebuild: (T, T) => Unit) = throw exception
}
