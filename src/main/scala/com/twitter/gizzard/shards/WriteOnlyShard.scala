package com.twitter.gizzard.shards

import scala.collection.mutable

import nameserver.NameServer

class WriteOnlyShardFactory[ConcreteShard <: Shard](readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard) extends shards.ShardFactory[ConcreteShard] {
  def instantiate(nameServer: NameServer[ConcreteShard], shardInfo: shards.ShardInfo, weight: Int, children: Seq[ConcreteShard]) =
    readWriteShardAdapter(new WriteOnlyShard(shardInfo, weight, children))
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class WriteOnlyShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[ConcreteShard])
  extends ReadWriteShard[ConcreteShard] {

  val shard = children.first

  override def readOperation[A](method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is write-only")

  override def writeOperation[A](method: (ConcreteShard => A)) = method(shard)
}
