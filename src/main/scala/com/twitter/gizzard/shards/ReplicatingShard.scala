package com.twitter.gizzard.shards

import com.twitter.gizzard.nameserver.LoadBalancer


case class ReplicatingShard[T](
  val shardInfo: ShardInfo,
  val weight: Int,
  val children: Seq[RoutingNode[T]])
extends RoutingNode[T] {

  protected def loadBalancer = new LoadBalancer(children)

  protected[shards] def collectedShards = loadBalancer() flatMap { _.collectedShards }

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
