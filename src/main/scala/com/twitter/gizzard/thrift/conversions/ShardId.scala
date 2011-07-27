package com.twitter.gizzard.thrift.conversions

import com.twitter.gizzard.shards
import com.twitter.gizzard.thrift


object ShardId {
  class RichShardId(shardId: shards.ShardId) {
    def toThrift = new thrift.ShardId(shardId.hostname, shardId.tablePrefix)
  }
  implicit def shardIdToRichShardId(shardId: shards.ShardId) = new RichShardId(shardId)

  class RichThriftShardId(shardId: thrift.ShardId) {
    def fromThrift = new shards.ShardId(shardId.hostname, shardId.table_prefix)
  }
  implicit def thriftShardIdToRichShardId(shardId: thrift.ShardId) = new RichThriftShardId(shardId)
}
