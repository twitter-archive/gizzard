package com.twitter.gizzard.nameserver

import gizzard.shards.{ShardId, ShardInfo, LinkInfo}
import thrift.conversions.ShardInfo._
import thrift.conversions.LinkInfo._
import thrift.conversions.Forwarding._
import thrift.conversions.Sequences._
import scala.collection.mutable.ListBuffer

case class NameServerState(shards: List[ShardInfo], links: List[LinkInfo], forwardings: List[Forwarding], tableId: Int) {
  def toThrift = {
    val thriftForwardings = forwardings.map(_.toThrift).toJavaList
    val thriftLinks       = links.map(_.toThrift).toJavaList
    val thriftShards      = shards.map(_.toThrift).toJavaList
    new thrift.NameServerState(thriftShards, thriftLinks, thriftForwardings, tableId)
  }
}

object NameServerState {
  import TreeUtils._

  def extractTable(tableId: Int)
                  (forwardingsByTable: Int => Set[Forwarding])
                  (linksByUpId: ShardId => Set[LinkInfo])
                  (shardsById: ShardId => ShardInfo) = {

    val forwardings = forwardingsByTable(tableId)
    val links       = descendantLinks(forwardings.map(_.shardId))(linksByUpId)
    val shards      = (forwardings.map(_.shardId) ++ links.map(_.downId)).map(shardsById)

    NameServerState(shards.toList, links.toList, forwardings.toList, tableId)
  }
}
