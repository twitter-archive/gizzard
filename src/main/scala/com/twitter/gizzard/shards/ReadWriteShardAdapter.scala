package com.twitter.gizzard.shards


abstract class ReadWriteShardAdapter(shard: Shard) extends Shard {
  def weight = shard.weight
  def children = shard.children
  def shardInfo = shard.shardInfo
}