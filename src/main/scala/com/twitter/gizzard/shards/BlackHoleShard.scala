package com.twitter.gizzard.shards

class BlackHoleShardFactory[ConcreteShard <: Shard](readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard) extends shards.ShardFactory[ConcreteShard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[ConcreteShard]) =
    readWriteShardAdapter(new BlackHoleShard[ConcreteShard](shardInfo, weight, children))
  def materialize(shardInfo: shards.ShardInfo) = ()
}

/**
 * A shard that refuses all read/write traffic in a silent way.
 */
class BlackHoleShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard])
  extends ReadWriteShard[ConcreteShard] {

  val exception = new ShardBlackHoleException(shardInfo.id)

  def readAllOperation[A](method: (ConcreteShard => A)) = throw exception

  def readOperation[A](method: (ConcreteShard => A)) = throw exception

  def writeOperation[A](method: (ConcreteShard => A)) = throw exception

  def rebuildableReadOperation[A](method: (ConcreteShard => Option[A]))(rebuild: (ConcreteShard, ConcreteShard) => Unit) =
    throw exception
}
