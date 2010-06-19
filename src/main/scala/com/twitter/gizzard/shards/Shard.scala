package com.twitter.gizzard.shards

trait Shard {
  def shardInfo: ShardInfo
  def weight: Int
  def children: Seq[Shard]
  
  def equalsOrContains(other: Shard): Boolean = {
    other == this || children.exists(_.equalsOrContains(other))
  }
}
