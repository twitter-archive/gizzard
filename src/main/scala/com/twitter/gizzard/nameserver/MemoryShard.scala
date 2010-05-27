package com.twitter.gizzard.nameserver

import scala.collection.Map
import scala.collection.mutable
import scheduler.JobScheduler
import shards._


/**
 * NameServer implementation that doesn't actually store anything anywhere.
 * Useful for tests or stubbing out the partitioning scheme.
 */
class MemoryShard extends Shard {
  val children = List()
  val shardInfo = new ShardInfo("com.twitter.gizzard.nameserver.MemoryShard", "", "")
  val weight = 1 // hardcode for now

  val shardTable = new mutable.ListBuffer[ShardInfo]()
  val parentTable = new mutable.HashMap[Int, (Int, Int)]()
  val forwardingTable = new mutable.ListBuffer[Forwarding]()

  private def find(info: ShardInfo): Option[ShardInfo] = {
    shardTable.find { x =>
      x.tablePrefix == info.tablePrefix && x.hostname == info.hostname
    }
  }

  private def find(shardId: Int): Option[ShardInfo] = {
    shardTable.find { _.shardId == shardId }
  }

  private def sortedChildren(list: List[ChildInfo]): List[ChildInfo] = {
    list.sort { (a, b) => a.weight > b.weight || (a.weight == b.weight && a.shardId > b.shardId) }
  }

  def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S]): Int = {
    find(shardInfo) match {
      case Some(x) =>
        if (x.className != shardInfo.className ||
            x.sourceType != shardInfo.sourceType ||
            x.destinationType != shardInfo.destinationType) {
          throw new InvalidShard
        }
      case None =>
        shardTable += shardInfo.clone
    }
    repository.create(shardInfo)
    shardInfo.shardId
  }

  def findShard(shardInfo: ShardInfo): Int = {
    find(shardInfo).map { _.shardId }.getOrElse { throw new NonExistentShard }
  }

  def getShard(shardId: Int): ShardInfo = {
    find(shardId).getOrElse { throw new NonExistentShard }
  }

  def updateShard(shardInfo: ShardInfo) = {
    find(shardInfo.shardId) match {
      case Some(x) =>
        shardTable -= x
        shardTable += shardInfo.clone
      case None =>
        throw new NonExistentShard
    }
  }

  def deleteShard(shardId: Int) {
    parentTable.elements.toList.foreach { case (child, (parent, weight)) =>
      if (parent == shardId || child == shardId) {
        parentTable -= child
      }
    }
    find(shardId).foreach { x => shardTable -= x }
  }

  def addChildShard(parentShardId: Int, childShardId: Int, weight: Int) {
    parentTable(childShardId) = (parentShardId, weight)
  }

  def removeChildShard(parentShardId: Int, childShardId: Int) {
    parentTable.elements.toList.foreach { case (child, (parent, weight)) =>
      if (parentShardId == parent && childShardId == child) {
        parentTable -= child
      }
    }
  }

  def replaceChildShard(oldChildShardId: Int, newChildShardId: Int) {
    parentTable.get(oldChildShardId).foreach { case (parent, weight) =>
      parentTable -= oldChildShardId
      parentTable(newChildShardId) = (parent, weight)
    }
  }

  def markShardBusy(shardId: Int, busy: Busy.Value) {
    find(shardId).foreach { _.busy = busy }
  }

  def setForwarding(forwarding: Forwarding) {
    forwardingTable.find { x =>
      x.baseId == forwarding.baseId && x.tableId == forwarding.tableId
    }.foreach { forwardingTable -= _ }
    forwardingTable += forwarding
  }

  def replaceForwarding(oldShardId: Int, newShardId: Int) {
    forwardingTable.find { x =>
      x.shardId == oldShardId
    }.foreach { x =>
      forwardingTable -= x
      forwardingTable += Forwarding(x.tableId, x.baseId, newShardId)
    }
  }

  def getForwarding(tableId: Int, baseId: Long): ShardInfo = {
    forwardingTable.find { x =>
      x.tableId == tableId && x.baseId == baseId
    }.map { x => getShard(x.shardId) }.getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardingForShard(shardId: Int): Forwarding = {
    forwardingTable.find { x =>
      x.shardId == shardId
    }.getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardings(): Seq[Forwarding] = {
    forwardingTable.toList
  }

  def shardIdsForHostname(hostname: String, className: String): List[Int] = {
    shardsForHostname(hostname, className).map { _.shardId }
  }

  def listShards(): Seq[ShardInfo] = {
    shardTable.toList
  }

  def listShardChildren(): Map[Int, Seq[ChildInfo]] = {
    val map = new mutable.HashMap[Int, mutable.ListBuffer[ChildInfo]]()
    parentTable.foreach { case (child, (parent, weight)) =>
      map.getOrElseUpdate(parent, new mutable.ListBuffer[ChildInfo]()) += ChildInfo(child, weight)
    }
    map.toList.foreach { case (k, v) =>
      val x = new mutable.ListBuffer[ChildInfo]()
      x ++= sortedChildren(v.toList)
      map(k) = x
    }
    map
  }

  def shardsForHostname(hostname: String, className: String): List[ShardInfo] = {
    shardTable.filter { x =>
      x.hostname == hostname && x.className == className
    }.toList
  }

  def getBusyShards(): Seq[ShardInfo] = {
    shardTable.filter { _.busy.id > 0 }.toList
  }

  def getParentShard(shardId: Int): ShardInfo = {
    parentTable.get(shardId) match {
      case Some((parent, weight)) => find(parent).get
      case None => find(shardId).get
    }
  }

  def getRootShard(shardId: Int): ShardInfo = {
    parentTable.get(shardId) match {
      case Some((parent, weight)) => getRootShard(parent)
      case None => find(shardId).get
    }
  }

  def getChildShardsOfClass(parentShardId: Int, className: String): List[ShardInfo] = {
    val childIds = listShardChildren(parentShardId)
    childIds.map { child => getShard(child.shardId) }.filter { _.className == className }.toList ++
      childIds.flatMap { child => getChildShardsOfClass(child.shardId, className) }
  }

  def rebuildSchema() { }

  def reload() { }

  def listShardChildren(parentId: Int): Seq[ChildInfo] = {
    val rv = new mutable.ListBuffer[ChildInfo]
    parentTable.foreach { case (child, (parent, weight)) =>
      if (parent == parentId) {
        rv += ChildInfo(child, weight)
      }
    }
    sortedChildren(rv.toList)
  }
}
