package com.twitter.gizzard
package shards

import java.lang.reflect.UndeclaredThrowableException
import java.sql.SQLException
import java.util.Random
import java.util.concurrent.{ExecutionException, TimeoutException, TimeUnit}
import scala.collection.mutable
import scala.util.Sorting
import com.twitter.gizzard.nameserver.LoadBalancer
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.util.Duration
import com.twitter.logging.Logger


class ReplicatingShardFactory[T](future: Option[Future]) extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, replicas: Seq[RoutingNode[T]]) = {
    new ReplicatingShard(
      shardInfo,
      weight,
      replicas,
      new LoadBalancer(replicas),
      future
    )
  }
}

class ReplicatingShard[T](
  val shardInfo: ShardInfo,
  val weight: Int,
  val children: Seq[RoutingNode[T]],
  val loadBalancer: (() => Seq[RoutingNode[T]]),
  val future: Option[Future])
extends RoutingNode[T] {

  import RoutingNode._

  protected[shards] def collectedShards = loadBalancer() flatMap { _.collectedShards }

  override def skip(ss: ShardId*) = {
    val toSkip = ss.toSet
    val filtered = children.filterNot { c => toSkip contains c.shardInfo.id }

    if (filtered.isEmpty) {
      BlackHoleShard(shardInfo, weight, Seq(this))
    } else if (filtered.size == children.size) {
      this
    } else {
      new ReplicatingShard[T](shardInfo, weight, filtered, new LoadBalancer(filtered), future)
    }
  }
}
