package com.twitter.gizzard.nameserver

import scala.util.Random
import scala.collection.mutable.ArrayBuffer

class LoadBalancer[ConcreteShard <: shards.Shard](
      random: Random,
      replicas: Seq[ConcreteShard])
  extends (() => Seq[ConcreteShard]) {

  def this(replicas: Seq[ConcreteShard]) = this(new Random, replicas)

  def apply() = sort(replicas)

  protected def sort(replicas: Seq[ConcreteShard]): List[ConcreteShard] =
    sort(replicas.foldLeft(0)(_ + _.weight), replicas)

  protected def sort(totalWeight: Int, replicas: Seq[ConcreteShard]): List[ConcreteShard] = replicas match {
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

class FailingOverLoadBalancer[ConcreteShard <: shards.Shard](
      random: Random,
      replicas: Seq[ConcreteShard])
  extends LoadBalancer[ConcreteShard](random, replicas) {

  def this(replicas: Seq[ConcreteShard]) = this(new Random, replicas)

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

  private def randomize(replicas: Seq[ConcreteShard]) = {
    val buffer = new ArrayBuffer[ConcreteShard]
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
