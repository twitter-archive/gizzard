package com.twitter.gizzard.sharding

import scala.collection.mutable
import gen.ShardInfo


abstract class ReadOnlyShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, children: Seq[ConcreteShard])
  extends ReadWriteShard[ConcreteShard] {

  val shard = children.first

  def readOperation[A](method: (ConcreteShard => A)) = method(shard)

  def writeOperation[A](method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is read-only")
}
