package com.twitter.gizzard.thrift.conversions

import com.twitter.gizzard.thrift.conversions.Sequences._


object Forwarding {
  class RichShardingForwarding(forwarding: nameserver.Forwarding) {
    def toThrift = new thrift.Forwarding(forwarding.serviceId, forwarding.tableId, forwarding.baseId, forwarding.shardId)
  }
  implicit def shardingForwardingToRichShardingForwarding(forwarding: nameserver.Forwarding) = new RichShardingForwarding(forwarding)

  class RichThriftForwarding(forwarding: thrift.Forwarding) {
    def fromThrift = new nameserver.Forwarding(forwarding.service_id, forwarding.table_id, forwarding.base_id, forwarding.shard_id)
  }
  implicit def thriftForwardingToRichThriftForwarding(forwarding: thrift.Forwarding) = new RichThriftForwarding(forwarding)
}
