package com.twitter.gizzard.nameserver

import java.util.TreeMap
import scala.collection.mutable
import shards._


class CachingNameServer[S <: Shard](nameServer: ManagingNameServer, shardRepository: ShardRepository[S], mappingFunction: Long => Long)
  extends ForwardingNameServer with ManagingNameServer {
  val children = List()
  val shardInfo = new ShardInfo("com.twitter.gizzard.nameserver.CachingNameServer", "", "")
  val weight = 1 // hardcode for now

  @volatile protected var shardInfos = mutable.Map.empty[Int, ShardInfo]
  @volatile private var familyTree: scala.collection.Map[Int, Seq[ChildInfo]] = null
  @volatile private var forwardings: scala.collection.Map[Int, TreeMap[Long, ShardInfo]] = null

  reload()

  def getShardInfo(id: Int) = shardInfos(id)

  def getChildren(id: Int) = {
    familyTree.getOrElse(id, new mutable.ArrayBuffer[ChildInfo])
  }

  def reload() {
    val newShardInfos = mutable.Map.empty[Int, ShardInfo]
    nameServer.listShards().foreach { shardInfo =>
      newShardInfos += (shardInfo.shardId -> shardInfo)
    }

    val newFamilyTree = nameServer.listShardChildren()

    val newForwardings = new mutable.HashMap[Int, TreeMap[Long, ShardInfo]]
    nameServer.getForwardings().foreach { forwarding =>
      val treeMap = newForwardings.getOrElseUpdate(forwarding.tableId, new TreeMap[Long, ShardInfo])
      treeMap.put(forwarding.baseId, newShardInfos.getOrElse(forwarding.shardId, throw new NonExistentShard))
    }

    shardInfos = newShardInfos
    familyTree = newFamilyTree
    forwardings = newForwardings
  }

  def findShardById(shardId: Int, weight: Int): S = {
    val shardInfo = getShardInfo(shardId)
    val children = getChildren(shardId).map { childInfo =>
      findShardById(childInfo.shardId, childInfo.weight)
    }.toList
    shardRepository.find(shardInfo, weight, children)
  }

  def findShardById(shardId: Int): S = findShardById(shardId, 1)

  def findCurrentForwarding(tableId: Int, id: Long) = {
    forwardings.get(tableId).flatMap { bySourceIds =>
      val item = bySourceIds.floorEntry(mappingFunction(id))
      if (item != null) {
        Some(item.getValue)
      } else {
        None
      }
    } getOrElse {
      throw new NonExistentShard
    }
  }

  def listShardChildren(parentId: Int) = nameServer.listShardChildren(parentId)
  def createShard(shardInfo: ShardInfo) = nameServer.createShard(shardInfo)
  def findShard(shardInfo: ShardInfo) = nameServer.findShard(shardInfo)
  def getShard(shardId: Int) = nameServer.getShard(shardId)
  def updateShard(shardInfo: ShardInfo) = nameServer.updateShard(shardInfo)
  def deleteShard(shardId: Int) = nameServer.deleteShard(shardId)
  def addChildShard(parentShardId: Int, childShardId: Int, weight: Int) = nameServer.addChildShard(parentShardId, childShardId, weight)
  def removeChildShard(parentShardId: Int, childShardId: Int) = nameServer.removeChildShard(parentShardId, childShardId)
  def replaceChildShard(oldChildShardId: Int, newChildShardId: Int) = nameServer.replaceChildShard(oldChildShardId, newChildShardId)
  def markShardBusy(shardId: Int, busy: Busy.Value) = nameServer.markShardBusy(shardId, busy)
  def setForwarding(forwarding: Forwarding) = nameServer.setForwarding(forwarding)
  def replaceForwarding(oldShardId: Int, newShardId: Int) = nameServer.replaceForwarding(oldShardId, newShardId)
  def getForwarding(tableId: Int, baseId: Long) = nameServer.getForwarding(tableId, baseId)
  def getForwardingForShard(shardId: Int) = nameServer.getForwardingForShard(shardId)
  def getForwardings() = nameServer.getForwardings()
  def shardIdsForHostname(hostname: String, className: String) = nameServer.shardIdsForHostname(hostname, className)
  def listShards() = nameServer.listShards()
  def listShardChildren() = nameServer.listShardChildren()
  def shardsForHostname(hostname: String, className: String) = nameServer.shardsForHostname(hostname, className)
  def getBusyShards() = nameServer.getBusyShards()
  def getParentShard(shardId: Int) = nameServer.getParentShard(shardId)
  def getRootShard(shardId: Int) = nameServer.getRootShard(shardId)
  def getChildShardsOfClass(parentShardId: Int, className: String) = nameServer.getChildShardsOfClass(parentShardId, className)
  def rebuildSchema() = nameServer.rebuildSchema()
}
