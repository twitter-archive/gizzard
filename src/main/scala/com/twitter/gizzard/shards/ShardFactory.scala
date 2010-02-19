package com.twitter.gizzard.shards


trait ShardFactory[S <: Shard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[S]): S
  def materialize(shardInfo: ShardInfo)
}
