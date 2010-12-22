package com.twitter.gizzard.nameserver

import gizzard.shards.ShardId
import thrift.conversions.ShardInfo._
import thrift.conversions.LinkInfo._
import thrift.conversions.Forwarding._
import thrift.conversions.Sequences._
import scala.collection.mutable.ListBuffer

class NameserverState(initialShards: List[gizzard.shards.ShardInfo],
                      initialLinks: List[gizzard.shards.LinkInfo],
                      initialForwardings: List[Forwarding], tableId: Int) {
                             
  private val shardsById    = initialShards.foldLeft(Map.empty[gizzard.shards.ShardId, gizzard.shards.ShardInfo]) { (map, shard) => map + ((shard.id, shard)) }
  
  private val linksByParent = initialLinks.foldLeft(Map[gizzard.shards.ShardId, List[gizzard.shards.LinkInfo]]()) { (map, link) => 
    val key = link.upId
    val entry = map.getOrElse(key, List[gizzard.shards.LinkInfo]())
    
    map + ((key, entry ::: List(link)))
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
