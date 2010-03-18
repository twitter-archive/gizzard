package com.twitter.gizzard.shards


trait ShardFactory[S <: Shard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[Shard]): S
  def materialize(shardInfo: ShardInfo)
}

trait ShardMaterializer {
  def materialize(shardInfo: ShardInfo)
}
