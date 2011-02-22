package com.twitter.gizzard
package shards

import scala.collection.mutable


class WriteOnlyShardFactory[ConcreteShard <: Shard](readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard) extends shards.ShardFactory[ConcreteShard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[ConcreteShard]) =
    readWriteShardAdapter(new WriteOnlyShard(shardInfo, weight, children))
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class WriteOnlyShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[ConcreteShard])
  extends ReadWriteShard[ConcreteShard] {

  val shard = children.head
  private def throwException = throw new ShardRejectedOperationException("shard is write-only", shardInfo.id)

  def readAllOperation[A](method: (ConcreteShard => A)) = try { throwException } catch { case e => Seq(Left(e)) }
  def readOperation[A](method: (ConcreteShard => A))    = throwException

  def writeOperation[A](method: (ConcreteShard => A)) = method(shard)

  def rebuildableReadOperation[A](method: (ConcreteShard => Option[A]))(rebuild: (ConcreteShard, ConcreteShard) => Unit) =
    throwException
}
