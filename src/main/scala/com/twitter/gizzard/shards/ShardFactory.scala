package com.twitter.gizzard.shards


trait ShardFactory[ConcreteShard <: Shard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[ConcreteShard]): ConcreteShard
  def materialize(shardInfo: ShardInfo)
}
