package com.twitter.gizzard.thrift.conversions

import com.twitter.gizzard.thrift.conversions.Busy._


object ShardInfo {
  class RichShardingShardInfo(shardInfo: sharding.ShardInfo) {
    def toThrift = new thrift.ShardInfo(shardInfo.className, shardInfo.tablePrefix, shardInfo.hostname,
                                        shardInfo.sourceType, shardInfo.destinationType,
                                        shardInfo.busy.toThrift, shardInfo.shardId)
    
  }
  implicit def shardingShardInfoToRichShardingShardInfo(shardInfo: sharding.ShardInfo) = new RichShardingShardInfo(shardInfo)

  class RichThriftShardInfo(shardInfo: thrift.ShardInfo) {
    def fromThrift = new sharding.ShardInfo(shardInfo.class_name, shardInfo.table_prefix, shardInfo.hostname,
                                            shardInfo.source_type, shardInfo.destination_type,
                                            shardInfo.busy.fromThrift, shardInfo.shard_id)
    
  }
  implicit def thriftShardInfoToRichThriftShardInfo(shardInfo: thrift.ShardInfo) = new RichThriftShardInfo(shardInfo)
}