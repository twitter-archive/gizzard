package com.twitter.gizzard.sharding

import java.sql.SQLException
import gen.ShardInfo


trait Shard {
  def shardInfo: ShardInfo
  def weight: Int
}

class ShardRejectedOperationException(description: String) extends SQLException(description)
