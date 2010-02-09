package com.twitter.gizzard.sharding

import gen.ShardInfo


abstract class BlockedShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, children: Seq[ConcreteShard])
  extends ReadWriteShard[ConcreteShard] {

  val shard = children.first

  def readOperation[A](method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is offline")

  def writeOperation[A](method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is offline")
}
