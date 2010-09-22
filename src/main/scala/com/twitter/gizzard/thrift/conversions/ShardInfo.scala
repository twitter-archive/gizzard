package com.twitter.gizzard.thrift.conversions

import conversions.ShardId._


object ShardInfo {
  class RichShardingShardInfo(shardInfo: shards.ShardInfo) {
    def toThrift = new thrift.ShardInfo(shardInfo.id.toThrift, shardInfo.className,
                                        shardInfo.sourceType, shardInfo.destinationType,
                                        shardInfo.busy.id, shardInfo.deleted.id)

  }
  implicit def shardingShardInfoToRichShardingShardInfo(shardInfo: shards.ShardInfo) = new RichShardingShardInfo(shardInfo)

  class RichThriftShardInfo(shardInfo: thrift.ShardInfo) {
    def fromThrift = shards.ShardInfo(shardInfo.id.fromThrift, shardInfo.class_name, shardInfo.source_type,
                                          shardInfo.destination_type, shards.Busy(shardInfo.busy), shards.Deleted(shardInfo.deleted))

  }
  implicit def thriftShardInfoToRichThriftShardInfo(shardInfo: thrift.ShardInfo) = new RichThriftShardInfo(shardInfo)
}
