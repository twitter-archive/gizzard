package com.twitter.gizzard.shards


trait Shard {
  def shardInfo: ShardInfo
  def weight: Int
  def children: Seq[Shard]
}
