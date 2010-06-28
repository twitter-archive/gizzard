package com.twitter.gizzard.nameserver

import scala.util.Random


class LoadBalancer[ConcreteShard <: shards.Shard](random: Random, replicas: Seq[ConcreteShard]) extends (() => Seq[ConcreteShard]) {
  def this(replicas: Seq[ConcreteShard]) = this(new Random, replicas)
  val totalWeight = replicas.foldLeft(0) { _ + _.weight }

  def apply() = sort(totalWeight, replicas)

  private def sort(totalWeight: Int, replicas: Seq[ConcreteShard]): List[ConcreteShard] = replicas match {
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
