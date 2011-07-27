package com.twitter.gizzard.nameserver

import scala.collection.mutable
import com.twitter.gizzard.shards._


/**
 * NameServer implementation that doesn't actually store anything anywhere.
 * Useful for tests or stubbing out the partitioning scheme.
 */
class MemoryShardManagerSource extends ShardManagerSource {

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
    list.sortWith { (a, b) =>
      a.weight > b.weight || (a.weight == b.weight && a.downId.hashCode > b.downId.hashCode)
    }
  }

  def currentState() = {
    val tableIds = forwardingTable.map(_.tableId).toSet
    dumpStructure(tableIds.toSeq)
  }

  def createShard(shardInfo: ShardInfo) {
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
  }

  def getShard(shardId: ShardId): ShardInfo = {
    find(shardId).getOrElse { throw new NonExistentShard("Shard not found: %s".format(shardId)) }
  }

  def deleteShard(shardId: ShardId) {
    parentTable.iterator.toList.foreach { link =>
      if (link.upId == shardId || link.downId == shardId) {
        parentTable -= link
      }
    }
    find(shardId).foreach { x => shardTable -= x }
  }

  def addLink(upId: ShardId, downId: ShardId, weight: Int) {
    removeLink(upId, downId)
    parentTable += LinkInfo(upId, downId, weight)
  }

  def removeLink(upId: ShardId, downId: ShardId) {
    parentTable.iterator.toList.foreach { link =>
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

  def getForwardingsForTableIds(tableIds: Seq[Int]) = {
    val tableIdsSet = tableIds.toSet
    forwardingTable.filter(f => tableIdsSet(f.tableId)).toList
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

  def listTables(): Seq[Int] = {
    forwardingTable.map(_.tableId).toSet.toSeq.sortWith((a,b) => a < b)
  }

  def reload() { }
}

class MemoryRemoteClusterManagerSource extends RemoteClusterManagerSource {
  val hostTable = new mutable.ListBuffer[Host]()

  // Remote Host Cluster Management

  private def findHost(hostname: String, port: Int) =
    hostTable.find(h => h.hostname == hostname && h.port == port)

  def addRemoteHost(host: Host) {
    removeRemoteHost(host.hostname, host.port)
    hostTable += host
  }

  def removeRemoteHost(hostname: String, port: Int) {
    findHost(hostname, port).foreach(hostTable -= _)
  }

  private def setHostsStatus(hosts: Iterable[Host], status: HostStatus.Value) {
    hosts.foreach { h =>
      hostTable -= h
      addRemoteHost(new Host(h.hostname, h.port, h.cluster, status))
    }
  }

  def setRemoteHostStatus(hostname: String, port: Int, status: HostStatus.Value) =
    setHostsStatus(List(getRemoteHost(hostname, port)), status)

  def setRemoteClusterStatus(cluster: String, status: HostStatus.Value) =
    setHostsStatus(hostTable.filter(_.cluster == cluster), status)


  def getRemoteHost(hostname: String, port: Int) =
    findHost(hostname, port).getOrElse(throw new ShardException("No such remote host"))

  def listRemoteClusters()                = (Set() ++ hostTable.map(_.cluster)).toList
  def listRemoteHosts()                   = hostTable.toList
  def listRemoteHostsInCluster(c: String) = hostTable.filter(_.cluster == c).toList

  def reload() { }
}
