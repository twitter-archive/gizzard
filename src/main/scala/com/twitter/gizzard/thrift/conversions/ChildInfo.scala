package com.twitter.gizzard.thrift.conversions


object ChildInfo {
  class RichShardingChildInfo(childInfo: shards.ChildInfo) {
    def toThrift = new thrift.ChildInfo(childInfo.shardId, childInfo.weight)
  }
  implicit def shardingChildInfoToRichShardingChildInfo(childInfo: shards.ChildInfo) = new RichShardingChildInfo(childInfo)

  class RichThriftChildInfo(childInfo: thrift.ChildInfo) {
    def fromThrift = new shards.ChildInfo(childInfo.shard_id, childInfo.weight)
  }
  implicit def thriftChildInfoToRichThriftChildInfo(childInfo: thrift.ChildInfo) = new RichThriftChildInfo(childInfo)
}
