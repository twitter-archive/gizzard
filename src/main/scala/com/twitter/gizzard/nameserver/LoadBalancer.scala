package com.twitter.gizzard.nameserver

import shards.Shard
import scala.util.Random


class LoadBalancer(random: Random, replicas: Seq[Shard]) {
  def this(replicas: Seq[Shard]) = this(new Random, replicas)
  val totalWeight = replicas.foldLeft(0) { _ + _.weight }

  def apply() = sort(totalWeight, replicas)

  private def sort(totalWeight: Int, replicas: Seq[Shard]): List[Shard] = replicas match {
    case Seq() => Nil
    case Seq(first, rest @ _*) =>
      val remainingWeight = totalWeight - first.weight

      if (first.weight == 0) {
        sort(remainingWeight, rest)
      } else if (random.nextInt(totalWeight) <= first.weight - 1) {
        first :: sort(remainingWeight, rest)
      } else {
        sort(remainingWeight, rest) ++ List(first)
      }
  }
}
