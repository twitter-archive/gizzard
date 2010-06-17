package com.twitter.gizzard.shards

import com.twitter.ostrich.StatsProvider


class StatsCollectingShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard], stats: StatsProvider)
  extends ReadWriteShard[ConcreteShard] {

  val shardName = shardInfo.hostname
  val shard = children.first.asInstanceOf[ConcreteShard]
  override def readOperation[A](method: (ConcreteShard => A)) = stats.time("shard-" + shardName + "-read")(method(shard))
  override def writeOperation[A](method: (ConcreteShard => A)) = stats.time("shard-" + shardName + "-write")(method(shard))
}
