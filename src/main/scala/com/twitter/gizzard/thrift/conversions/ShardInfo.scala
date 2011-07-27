package com.twitter.gizzard.thrift.conversions

import com.twitter.gizzard.shards
import com.twitter.gizzard.thrift
import com.twitter.gizzard.thrift.conversions.Busy._
import com.twitter.gizzard.thrift.conversions.ShardId._


object ShardInfo {
  class RichShardingShardInfo(shardInfo: shards.ShardInfo) {
    def toThrift = new thrift.ShardInfo(shardInfo.id.toThrift, shardInfo.className,
                                        shardInfo.sourceType, shardInfo.destinationType,
                                        shardInfo.busy.toThrift)

  }
  implicit def shardingShardInfoToRichShardingShardInfo(shardInfo: shards.ShardInfo) = new RichShardingShardInfo(shardInfo)

  class RichThriftShardInfo(shardInfo: thrift.ShardInfo) {
    def fromThrift = new shards.ShardInfo(shardInfo.id.fromThrift, shardInfo.class_name, shardInfo.source_type,
                                          shardInfo.destination_type, shardInfo.busy.fromThrift)

  }
  implicit def thriftShardInfoToRichThriftShardInfo(shardInfo: thrift.ShardInfo) = new RichThriftShardInfo(shardInfo)
}
