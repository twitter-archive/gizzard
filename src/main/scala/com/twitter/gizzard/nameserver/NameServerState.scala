package com.twitter.gizzard.nameserver

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import com.twitter.gizzard.shards.{ShardId, ShardInfo, LinkInfo}
import com.twitter.gizzard.thrift.{NameServerState => ThriftNameServerState}
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import com.twitter.gizzard.thrift.conversions.LinkInfo._
import com.twitter.gizzard.thrift.conversions.Forwarding._
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.util.TreeUtils


case class NameServerState(shards: List[ShardInfo], links: List[LinkInfo], forwardings: List[Forwarding], tableId: Int) {
  def toThrift = {
    val thriftForwardings = forwardings.map(_.toThrift)
    val thriftLinks       = links.map(_.toThrift)
    val thriftShards      = shards.map(_.toThrift)
    new ThriftNameServerState(thriftShards, thriftLinks, thriftForwardings, tableId)
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
