package com.twitter.gizzard.shards

import nameserver.NameServer

class BlockedShardFactory[ConcreteShard <: Shard](readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard) extends shards.ShardFactory[ConcreteShard] {
  def instantiate(nameServer: NameServer[ConcreteShard], shardInfo: shards.ShardInfo, weight: Int, children: Seq[ConcreteShard]) =
    readWriteShardAdapter(new BlockedShard(shardInfo, weight, children))
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class BlockedShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard])
  extends ReadWriteShard[ConcreteShard] {

  val shard = children.first

  def readOperation[A](id: Long, method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is offline")

  def writeOperation[A](id: Long, method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is offline")
}
