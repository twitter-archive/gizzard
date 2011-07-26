package com.twitter.gizzard.shards

import com.twitter.gizzard.nameserver.LoadBalancer


case class ReplicatingShard[T](
  val shardInfo: ShardInfo,
  val weight: Int,
  val children: Seq[RoutingNode[T]])
extends RoutingNode[T] {

  protected[shards] def collectedShards(readOnly: Boolean) = loadBalancer() flatMap { _.collectedShards(readOnly) }

  protected def loadBalancer() = new LoadBalancer(children).apply()
}
