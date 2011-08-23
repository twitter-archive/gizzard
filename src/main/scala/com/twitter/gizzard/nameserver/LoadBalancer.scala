package com.twitter.gizzard.nameserver

import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import com.twitter.gizzard.shards.RoutingNode


class LoadBalancer[T](
  random: Random,
  replicas: Seq[RoutingNode[T]])
extends (() => Seq[RoutingNode[T]]) {

  def this(replicas: Seq[RoutingNode[T]]) = this(new Random, replicas)

  def apply() = sort(replicas)

  protected def sort(replicas: Seq[RoutingNode[T]]): List[RoutingNode[T]] =
    sort(replicas.foldLeft(0)(_ + _.weight), replicas)

  protected def sort(totalWeight: Int, replicas: Seq[RoutingNode[T]]): List[RoutingNode[T]] = replicas match {
    case Seq() => Nil
    case Seq(first, rest @ _*) =>
      val remainingWeight = totalWeight - first.weight

      if (first.weight == 0) {
        sort(remainingWeight, rest)
      } else if (random.nextInt(totalWeight) < first.weight) {
        first :: sort(remainingWeight, rest)
      } else {
        sort(remainingWeight, rest) ++ List(first)
      }
  }
}
