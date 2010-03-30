package com.twitter.gizzard.shards


class BlockedShardFactory extends shards.ShardFactory[Shard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard]) =
    new BlockedShard(shardInfo, weight, children)
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class BlockedShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard])
  extends ReadWriteShard[ConcreteShard] {

  val shard = children.first

  def readOperation[A](method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is offline")

  def writeOperation[A](method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is offline")
}
