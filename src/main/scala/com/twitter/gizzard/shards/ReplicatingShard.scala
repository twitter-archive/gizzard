package com.twitter.gizzard.shards

import com.twitter.gizzard.nameserver.LoadBalancer


case class ReplicatingShard[T](
  val shardInfo: ShardInfo,
  val weight: Int,
  val children: Seq[RoutingNode[T]])
extends RoutingNode[T] {

  protected[shards] def collectedShards(readOnly: Boolean) = loadBalancer() flatMap { _.collectedShards(readOnly) }

  protected def loadBalancer() = new LoadBalancer(children).apply()

  override def skip(ss: ShardId*) = {
    val toSkip = ss.toSet
    val filtered = children.filterNot { c => toSkip contains c.shardInfo.id }

    if (filtered.isEmpty) {
      BlackHoleShard(shardInfo, weight, Seq(this))
    } else if (filtered.size == children.size) {
      this
    } else {
      new ReplicatingShard[T](shardInfo, weight, filtered)
    }
  }
}
