package com.twitter.gizzard.sharding


trait Shard {
  def shardInfo: ShardInfo
  def weight: Int
}

class ShardException(description: String) extends Exception(description)

class ShardTimeoutException extends ShardException("timeout")

class ShardRejectedOperationException(description: String) extends ShardException(description)
