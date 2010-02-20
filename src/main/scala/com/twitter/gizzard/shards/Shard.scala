package com.twitter.gizzard.shards


trait Shard {
  def shardInfo: ShardInfo
  def weight: Int
  def children: Seq[Shard]
}

class ShardException(description: String) extends Exception(description)

class ShardTimeoutException extends ShardException("timeout")

class ShardRejectedOperationException(description: String) extends ShardException(description)
