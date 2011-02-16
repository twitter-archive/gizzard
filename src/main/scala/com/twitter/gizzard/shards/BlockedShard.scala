package com.twitter.gizzard
package shards

class BlockedShardFactory[ConcreteShard <: Shard](readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard) extends shards.ShardFactory[ConcreteShard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[ConcreteShard]) =
    readWriteShardAdapter(new BlockedShard(shardInfo, weight, children))
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class BlockedShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard])
  extends ReadWriteShard[ConcreteShard] {

  val shard = children.first
  private def throwException = throw new ShardRejectedOperationException("shard is offline", shardInfo.id)

  def readAllOperation[A](method: (ConcreteShard => A)) = try { throwException } catch { case e => Seq(Left(e)) }
  def readOperation[A](method: (ConcreteShard => A))    = throwException
  def writeOperation[A](method: (ConcreteShard => A))   = throwException

  def rebuildableReadOperation[A](method: (ConcreteShard => Option[A]))(rebuild: (ConcreteShard, ConcreteShard) => Unit) =
    throwException
}
