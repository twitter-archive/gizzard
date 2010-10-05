package com.twitter.gizzard.shards

class BlockedShardFactory[ConcreteShard <: Shard](readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard) extends shards.VirtualShardFactory[ConcreteShard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[ConcreteShard]) =
    readWriteShardAdapter(new BlockedShard(shardInfo, weight, children))
}

class BlockedShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard])
  extends ReadWriteShard[ConcreteShard] {

  val shard = children.first

  def readOperation[A](method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is offline")

  def writeOperation[A](method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is offline")

  def rebuildableReadOperation[A](method: (ConcreteShard => Option[A]))(rebuild: (ConcreteShard, ConcreteShard) => Unit) =
    throw new ShardRejectedOperationException("shard is offline")
}
