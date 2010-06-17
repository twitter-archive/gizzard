package com.twitter.gizzard.nameserver

import shards._
import scala.collection.mutable
import java.util.TreeMap

class InvalidNameServer extends ShardException("Different number of forwardings/children")

class SplittingNameServer[S <: shards.Shard](
      baseNameServer: NameServer[S], baseShard: S, val children: List[S]) 
      extends Shard[S] {
  val weight = baseShard.weight;
  val shardInfo = baseShard.shardInfo;
  @volatile private var forwardings = new mutable.HashMap[Int, TreeMap[Long, ShardInfo]]
  @volatile private var forwardingMap = new mutable.HashMap[ShardId, Forwarding]
  @volatile private var shardMap = new mutable.HashMap[ShardId, S]
  
  val forwardingsList = baseNameServer.getForwardingsForShard(shardInfo.id).toList
  
  if(forwardingsList.size != children.size) {
    throw new InvalidNameServer
  }
    
  forwardingsList.zip(children).foreach { pair => 
    val (forwarding, child) = pair
    shardMap.put(child.shardInfo.id, child)
    forwardingMap.put(child.shardInfo.id, forwarding)
    val treeMap = forwardings.getOrElseUpdate(forwarding.tableId, new TreeMap[Long, ShardInfo])
    treeMap.put(forwarding.baseId, child.shardInfo)
  }  
  
  def getForwardingsForShard(id: ShardId): Seq[Forwarding] = {
    List(forwardingMap.get(id).getOrElse { throw new NonExistentShard })
  }
    
  //Unfortunate copy-pasta
  def findCurrentForwarding(address: (Int, Long)) = {
    val (tableId, id) = address
    val shardInfo = forwardings.get(tableId).flatMap { bySourceIds =>
      val item = bySourceIds.floorEntry(id)
      if (item != null) {
        Some(item.getValue)
      } else {
        None
      }
    } getOrElse {
      throw new NonExistentShard
    }

    findShardById(shardInfo.id)
  }
  
  def findShardById(id: ShardId) = {
    shardMap.get(id).getOrElse {
      throw new NonExistentShard
    }
  }
  
  @throws(classOf[shards.ShardException]) def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S])   = unsupported
  @throws(classOf[shards.ShardException]) def getShard(id: ShardId): ShardInfo                                                       = unsupported
  @throws(classOf[shards.ShardException]) def deleteShard(id: ShardId)                                                               = unsupported
  @throws(classOf[shards.ShardException]) def addLink(upId: ShardId, downId: ShardId, weight: Int)                                   = unsupported
  @throws(classOf[shards.ShardException]) def removeLink(upId: ShardId, downId: ShardId)                                             = unsupported
  @throws(classOf[shards.ShardException]) def listUpwardLinks(id: ShardId): Seq[LinkInfo]                                            = unsupported
  @throws(classOf[shards.ShardException]) def listDownwardLinks(id: ShardId): Seq[LinkInfo]                                          = unsupported
  @throws(classOf[shards.ShardException]) def markShardBusy(id: ShardId, busy: Busy.Value)                                           = unsupported
  @throws(classOf[shards.ShardException]) def setForwarding(forwarding: Forwarding)                                                  = unsupported
  @throws(classOf[shards.ShardException]) def replaceForwarding(oldId: ShardId, newId: ShardId)                                      = unsupported
  @throws(classOf[shards.ShardException]) def getForwarding(tableId: Int, baseId: Long): Forwarding                                  = unsupported
  @throws(classOf[shards.ShardException]) def getForwardings(): Seq[Forwarding]                                                      = unsupported
  @throws(classOf[shards.ShardException]) def shardsForHostname(hostname: String): Seq[ShardInfo]                                    = unsupported
  @throws(classOf[shards.ShardException]) def listShards(): Seq[ShardInfo]                                                           = unsupported
  @throws(classOf[shards.ShardException]) def listLinks(): Seq[LinkInfo]                                                             = unsupported
  @throws(classOf[shards.ShardException]) def getBusyShards(): Seq[ShardInfo]                                                        = unsupported
  @throws(classOf[shards.ShardException]) def getChildShardsOfClass(parentId: ShardId, className: String): Seq[ShardInfo]            = unsupported
  @throws(classOf[shards.ShardException]) def rebuildSchema()                                                                        = unsupported
  @throws(classOf[shards.ShardException]) def reload()                                                                               = unsupported

  private def unsupported = throw new UnsupportedOperationException()
}
