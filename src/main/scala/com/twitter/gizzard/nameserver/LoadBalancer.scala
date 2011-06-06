package com.twitter.gizzard
package nameserver

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

class FailingOverLoadBalancer[T](
  random: Random,
  replicas: Seq[RoutingNode[T]])
extends LoadBalancer[T](random, replicas) {

  def this(replicas: Seq[RoutingNode[T]]) = this(new Random, replicas)

  val keepWarmFalloverRate = 0.01

  val (online, offline) = replicas.partition(_.weight > 0)

  override def apply() = {
    val (head :: tail) = sort(online.toSeq)
    val offlineRandomized = randomize(offline.toSeq)

    if ( random.nextFloat <= keepWarmFalloverRate ) {
      offlineRandomized ++ (head :: tail)
    } else {
      head :: (offlineRandomized ++ tail)
    }
  }

  private def randomize(replicas: Seq[RoutingNode[T]]) = {
    val buffer = new ArrayBuffer[RoutingNode[T]]
    buffer ++= replicas

    def swap(a: Int, b: Int) {
      val tmp = buffer(a)
      buffer(a) = buffer(b)
      buffer(b) = tmp
    }

    for (n <- 1 to buffer.length) {
      swap(n - 1, random.nextInt(n))
    }

    buffer.toList
  }
}
