package com.twitter.gizzard.nameserver

import java.util.{Random, TreeMap}
import scala.collection.mutable
import com.twitter.xrayspecs.Time
import shards._


class NonExistentShard extends ShardException("Shard does not exist")
class InvalidShard extends ShardException("Shard has invalid attributes (such as hostname)")

class NameServer[S <: shards.Shard](nameServer: Shard, shardRepository: ShardRepository[S], mappingFunction: Long => Long)
  extends Shard {
  val children = List()
  val shardInfo = new ShardInfo("com.twitter.gizzard.nameserver.NameServer", "", "")
  val weight = 1 // hardcode for now
  val RETRIES = 5
  val random = new Random()

  @volatile protected var shardInfos = mutable.Map.empty[Int, ShardInfo]
  @volatile private var familyTree: scala.collection.Map[Int, Seq[ChildInfo]] = null
  @volatile private var forwardings: scala.collection.Map[Int, TreeMap[Long, ShardInfo]] = null

  private def nextId = {
    ((Time.now.inMillis & ((1 << 20) - 1)) | (random.nextInt() & 0xfffff)).toInt
  }

  def createShard(shardInfo: ShardInfo): Int = createShard(shardInfo, RETRIES)

  def createShard(shardInfo: ShardInfo, retries: Int): Int = {
    shardInfo.shardId = nextId
    try {
      nameServer.createShard(shardInfo, shardRepository)
    } catch {
      case e: InvalidShard if (retries > 0) =>
        // allow conflicts on the id generator
        createShard(shardInfo, retries - 1)
    }
  }

  def getShardInfo(id: Int) = shardInfos(id)

  def getChildren(id: Int) = {
    familyTree.getOrElse(id, new mutable.ArrayBuffer[ChildInfo])
  }

  def reload() {
    nameServer.reload()

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

  @throws(classOf[NonExistentShard])
  def findShardById(shardId: Int): S = findShardById(shardId, 1)

  def findCurrentForwarding(tableId: Int, id: Long) = {
    val shardInfo = forwardings.get(tableId).flatMap { bySourceIds =>
      val item = bySourceIds.floorEntry(mappingFunction(id))
      if (item != null) {
        Some(item.getValue)
      } else {
        None
      }
    } getOrElse {
      throw new NonExistentShard
    }

    findShardById(shardInfo.shardId)
  }

  def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S]) = nameServer.createShard(shardInfo, repository)
  def listShardChildren(parentId: Int) = nameServer.listShardChildren(parentId)
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
