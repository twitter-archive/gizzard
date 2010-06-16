package com.twitter.gizzard.shards

import scala.collection.mutable
import nameserver.NameServer

class ReadOnlyShardFactory[ConcreteShard <: Shard](readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard) extends shards.ShardFactory[ConcreteShard] {
  def instantiate(nameServer: NameServer[ConcreteShard], shardInfo: shards.ShardInfo, weight: Int, children: Seq[ConcreteShard]) =
    readWriteShardAdapter(new ReadOnlyShard(shardInfo, weight, children))
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class ReadOnlyShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[ConcreteShard])
  extends ReadWriteShard[ConcreteShard] {

  val shard = children.first

  def readOperation[A](id: Long, method: (ConcreteShard => A)) = method(shard)

  def writeOperation[A](id: Long, method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is read-only")
}
