package com.twitter.gizzard
package shards

import scala.collection.mutable


class ReadOnlyShardFactory[ConcreteShard <: Shard](readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard) extends ShardFactory[ConcreteShard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[ConcreteShard]) =
    readWriteShardAdapter(new ReadOnlyShard(shardInfo, weight, children))
  def materialize(shardInfo: ShardInfo) = ()
}

class ReadOnlyShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[ConcreteShard])
  extends ReadWriteShard[ConcreteShard] {

  val shard = children.head

  def readAllOperation[A](method: (ConcreteShard => A)) = Seq(try { Right(method(shard)) } catch { case e => Left(e) })

  def readOperation[A](method: (ConcreteShard => A)) = method(shard)

  def writeOperation[A](method: (ConcreteShard => A)) =
    throw new ShardRejectedOperationException("shard is read-only", shardInfo.id)

  def rebuildableReadOperation[A](method: (ConcreteShard => Option[A]))(rebuild: (ConcreteShard, ConcreteShard) => Unit) =
    method(shard)
}
