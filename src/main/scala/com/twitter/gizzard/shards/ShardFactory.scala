package com.twitter.gizzard.shards


trait ShardFactory[ConcreteShard <: Shard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[ConcreteShard]): ConcreteShard
  def materialize(shardInfo: ShardInfo)
  def purge(shardInfo: ShardInfo)
}

trait AbstractShardFactory[S <: Shard] extends ShardFactory[S] {
  def materialize(shardInfo: shards.ShardInfo) = ()
  def purge(shardInfo: shards.ShardInfo) = ()
}
