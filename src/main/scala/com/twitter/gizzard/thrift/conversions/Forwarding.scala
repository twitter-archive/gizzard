package com.twitter.gizzard
package thrift
package conversions

import conversions.Sequences._
import conversions.ShardId._


object Forwarding {
  class RichShardingForwarding(forwarding: nameserver.Forwarding) {
    def toThrift = new thrift.Forwarding(forwarding.tableId, forwarding.baseId, forwarding.shardId.toThrift)
  }
  implicit def shardingForwardingToRichShardingForwarding(forwarding: nameserver.Forwarding) = new RichShardingForwarding(forwarding)

  class RichThriftForwarding(forwarding: thrift.Forwarding) {
    def fromThrift = new nameserver.Forwarding(forwarding.table_id, forwarding.base_id, forwarding.shard_id.fromThrift)
  }
  implicit def thriftForwardingToRichThriftForwarding(forwarding: thrift.Forwarding) = new RichThriftForwarding(forwarding)
}
