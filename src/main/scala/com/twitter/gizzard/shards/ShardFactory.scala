package com.twitter.gizzard.shards


trait ShardFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int): T
  def materialize(shardInfo: ShardInfo)
}
