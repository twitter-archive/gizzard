package com.twitter.gizzard.nameserver

import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import com.twitter.gizzard.shards.RoutingNode

// The default load balancer will randomly shuffle the order of shards. It
// does not take weight into account.

class LoadBalancer[T](
  random: Random,
  replicas: Seq[RoutingNode[T]])
extends (() => Seq[RoutingNode[T]]) {

  def this(replicas: Seq[RoutingNode[T]]) = this(new Random, replicas)

  def apply() = sort(replicas)

  protected def sort(replicas: Seq[RoutingNode[T]]): List[RoutingNode[T]] = random.shuffle(replicas).toList
}
