package com.twitter.gizzard.sharding

import com.twitter.gizzard.Conversions._


case class Forwarding(tableId: List[Int], baseId: Long, shardId: Int)

object Forwarding {
  def toThrift(forwarding: Forwarding) =
    new thrift.Forwarding(forwarding.tableId.toJavaList, forwarding.baseId, forwarding.shardId)

  def fromThrift(forwarding: thrift.Forwarding) =
    new Forwarding(forwarding.table_id.toList, forwarding.base_id, forwarding.shard_id)
}
