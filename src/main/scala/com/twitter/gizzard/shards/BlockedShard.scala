package com.twitter.gizzard.shards

class BlockedShardFactory[ConcreteShard <: Shard](readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard) extends shards.ShardFactory[ConcreteShard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[ConcreteShard]) =
    readWriteShardAdapter(new BlockedShard(shardInfo, weight, children))
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class BlockedShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard])
  extends ReadWriteShard[ConcreteShard] {

  val shard = children.first

  def readOperation[A](address: Option[Address], method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is offline")

  def writeOperation[A](address: Option[Address], method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is offline")
}
