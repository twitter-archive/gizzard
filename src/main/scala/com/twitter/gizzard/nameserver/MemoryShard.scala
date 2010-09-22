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
  val parentTable = new mutable.ListBuffer[LinkInfo]()
  val forwardingTable = new mutable.ListBuffer[Forwarding]()

  private def find(info: ShardInfo): Option[ShardInfo] = {
    shardTable.find { x =>
      x.tablePrefix == info.tablePrefix && x.hostname == info.hostname
    }
  }

  private def find(shardId: ShardId): Option[ShardInfo] = {
    shardTable.find { _.id == shardId }
  }

  private def sortedLinks(list: List[LinkInfo]): List[LinkInfo] = {
    list.sort { (a, b) => a.weight > b.weight ||
                          (a.weight == b.weight && a.downId.hashCode > b.downId.hashCode) }
  }

  def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S]) {
    find(shardInfo) match {
      case Some(x) =>
        if (x.className != shardInfo.className ||
            x.sourceType != shardInfo.sourceType ||
            x.destinationType != shardInfo.destinationType) {
          throw new InvalidShard("Invalid shard: %s doesn't match %s".format(x, shardInfo))
        }
      case None =>
        shardTable += shardInfo.clone
    }
    repository.create(shardInfo)
  }

  def getShard(shardId: ShardId): ShardInfo = {
    find(shardId).getOrElse { throw new NonExistentShard("Shard not found: %s".format(shardId)) }
  }

  def deleteShard[S <: shards.Shard](shardId: ShardId, repository: ShardRepository[S]) {
    parentTable.elements.toList.foreach { link =>
      if (link.upId == shardId || link.downId == shardId) {
        parentTable -= link
      }
    }
    find(shardId).foreach { x => shardTable -= x }
  }

  def purgeShard[S <: shards.Shard](id: ShardId, repository: ShardRepository[S]) { }

  def getDeletedShards() = Nil

  def addLink(upId: ShardId, downId: ShardId, weight: Int) {
    removeLink(upId, downId)
    parentTable += LinkInfo(upId, downId, weight)
  }

  def removeLink(upId: ShardId, downId: ShardId) {
    parentTable.elements.toList.foreach { link =>
      if (upId == link.upId && downId == link.downId) {
        parentTable -= link
      }
    }
  }

  def listUpwardLinks(id: ShardId): Seq[LinkInfo] = {
    sortedLinks(parentTable.filter { link => link.downId == id }.toList)
  }

  def listDownwardLinks(id: ShardId): Seq[LinkInfo] = {
    sortedLinks(parentTable.filter { link => link.upId == id }.toList)
  }

  def markShardBusy(shardId: ShardId, busy: Busy.Value) {
    find(shardId).foreach { _.busy = busy }
  }

  def setForwarding(forwarding: Forwarding) {
    removeForwarding(forwarding)
    forwardingTable += forwarding
  }

  def removeForwarding(forwarding: Forwarding) = {
    forwardingTable.find { x =>
      x.baseId == forwarding.baseId && x.tableId == forwarding.tableId
    }.foreach { forwardingTable -= _ }
  }

  def replaceForwarding(oldShardId: ShardId, newShardId: ShardId) {
    forwardingTable.find { x =>
      x.shardId == oldShardId
    }.foreach { x =>
      forwardingTable -= x
      forwardingTable += Forwarding(x.tableId, x.baseId, newShardId)
    }
  }

  def getForwarding(tableId: Int, baseId: Long): Forwarding = {
    forwardingTable.find { x =>
      x.tableId == tableId && x.baseId == baseId
    }.getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardingForShard(shardId: ShardId): Forwarding = {
    forwardingTable.find { x =>
      x.shardId == shardId
    }.getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardings(): Seq[Forwarding] = {
    forwardingTable.toList
  }

  def listHostnames(): Seq[String] = {
    (Set() ++ shardTable.map { x => x.hostname }).toList
  }

  def shardsForHostname(hostname: String): Seq[ShardInfo] = {
    shardTable.filter { x => x.hostname == hostname }
  }

  def listShards(): Seq[ShardInfo] = {
    shardTable.toList
  }

  def listLinks(): Seq[LinkInfo] = {
    parentTable.toList
  }

  def getBusyShards(): Seq[ShardInfo] = {
    shardTable.filter { _.busy.id > 0 }.toList
  }

  def getChildShardsOfClass(parentShardId: ShardId, className: String): List[ShardInfo] = {
    val childIds = listDownwardLinks(parentShardId).map { _.downId }
    childIds.map { child => getShard(child) }.filter { _.className == className }.toList ++
      childIds.flatMap { child => getChildShardsOfClass(child, className) }
  }

  def rebuildSchema() { }

  def reload() { }
}
