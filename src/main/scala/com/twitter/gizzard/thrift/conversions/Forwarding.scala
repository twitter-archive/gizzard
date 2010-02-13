package com.twitter.gizzard.thrift.conversions

import com.twitter.gizzard.thrift.conversions.Sequences._


object Forwarding {
  class RichShardingForwarding(forwarding: sharding.Forwarding) {
    def toThrift = new thrift.Forwarding(forwarding.tableId.toJavaList, forwarding.baseId, forwarding.shardId)
  }
  implicit def shardingForwardingToRichShardingForwarding(forwarding: sharding.Forwarding) = new RichShardingForwarding(forwarding)

  class RichThriftForwarding(forwarding: thrift.Forwarding) {
    def fromThrift = new sharding.Forwarding(forwarding.table_id.toList, forwarding.base_id, forwarding.shard_id)
  }
  implicit def thriftForwardingToRichThriftForwarding(forwarding: thrift.Forwarding) = new RichThriftForwarding(forwarding)
}
