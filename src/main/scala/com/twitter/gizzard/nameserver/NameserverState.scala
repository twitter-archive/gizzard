package com.twitter.gizzard.nameserver

import gizzard.shards.{ShardId, ShardInfo, LinkInfo}
import thrift.conversions.ShardInfo._
import thrift.conversions.LinkInfo._
import thrift.conversions.Forwarding._
import thrift.conversions.Sequences._
import scala.collection.mutable.ListBuffer

class NameserverState(initialShards: List[gizzard.shards.ShardInfo],
                      initialLinks: List[gizzard.shards.LinkInfo],
                      initialForwardings: List[Forwarding], tableId: Int) {


  private val shardsById    = Map(initialShards.map(s => s.id -> s): _*)

  private val linksByParent = initialLinks.foldLeft(Map[ShardId, List[LinkInfo]]()) { (map, link) =>
    val key   = link.upId
    val entry = map.get(key).map(link :: _).getOrElse(List(link))

    map + (key -> entry)
  }

  val forwardings = initialForwardings.filter(_.tableId == tableId)

  var links = new ListBuffer[gizzard.shards.LinkInfo]
  var shards = new ListBuffer[gizzard.shards.ShardInfo]

  private def computeSubtree(id: ShardId): Unit = {
    shardsById.get(id).foreach { shardInfo =>
      shards += shardInfo
    }
    linksByParent.get(id).foreach { linksOption =>
      linksOption.foreach { link =>
        links += link
        computeSubtree(link.downId)
      }
    }
  }

  forwardings.foreach { forwarding => computeSubtree(forwarding.shardId) }

  def toThrift = {
    val thriftForwardings = forwardings.map(_.toThrift).toJavaList
    val thriftLinks =  links.map(_.toThrift).toJavaList
    val thriftShards = shards.map(_.toThrift).toJavaList
    new thrift.NameserverState(thriftShards, thriftLinks, thriftForwardings, tableId)
  }
}
