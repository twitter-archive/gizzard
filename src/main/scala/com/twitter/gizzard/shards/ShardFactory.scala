package com.twitter.gizzard.shards


trait ShardFactory[T] {
  def instantiate(shardInfo: ShardInfo): T
  def materialize(shardInfo: ShardInfo)
}
