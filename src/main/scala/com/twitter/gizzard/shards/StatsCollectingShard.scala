package com.twitter.gizzard.shards

import com.twitter.ostrich.StatsProvider


class StatsCollectingShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard], stats: StatsProvider)
  extends ReadWriteShard[ConcreteShard] {

  val shardName = shardInfo.hostname
  val shard = children.first.asInstanceOf[ConcreteShard]
  def readAllOperation[A](method: (ConcreteShard => A)) = stats.time("shard-" + shardName + "-readall")(Seq(method(shard)))
  def readOperation[A](method: (ConcreteShard => A)) = stats.time("shard-" + shardName + "-read")(method(shard))
  def writeOperation[A](method: (ConcreteShard => A)) = stats.time("shard-" + shardName + "-write")(method(shard))
  def rebuildableReadOperation[A](method: (ConcreteShard => Option[A]))(rebuild: (ConcreteShard, ConcreteShard) => Unit) =
    stats.time("shard-" + shardName + "-rebuildable") { method(shard) }
}
