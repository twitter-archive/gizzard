package com.twitter.gizzard
package nameserver

import scala.collection.mutable
import shards._

trait Shard {
  @throws(classOf[shards.ShardException]) def createShard[T](shardInfo: ShardInfo, repository: ShardRepository[T])
  @throws(classOf[shards.ShardException]) def deleteShard(id: ShardId)
  @throws(classOf[shards.ShardException]) def addLink(upId: ShardId, downId: ShardId, weight: Int)
  @throws(classOf[shards.ShardException]) def removeLink(upId: ShardId, downId: ShardId)
  @throws(classOf[shards.ShardException]) def setForwarding(forwarding: Forwarding)
  @throws(classOf[shards.ShardException]) def replaceForwarding(oldId: ShardId, newId: ShardId)
  @throws(classOf[shards.ShardException]) def removeForwarding(forwarding: Forwarding)

  @throws(classOf[shards.ShardException]) def getShard(id: ShardId): ShardInfo
  @throws(classOf[shards.ShardException]) def listUpwardLinks(id: ShardId): Seq[LinkInfo]
  @throws(classOf[shards.ShardException]) def listDownwardLinks(id: ShardId): Seq[LinkInfo]
  @throws(classOf[shards.ShardException]) def markShardBusy(id: ShardId, busy: Busy.Value)
  @throws(classOf[shards.ShardException]) def getForwarding(tableId: Int, baseId: Long): Forwarding
  @throws(classOf[shards.ShardException]) def getForwardingForShard(id: ShardId): Forwarding
  @throws(classOf[shards.ShardException]) def getForwardings(): Seq[Forwarding]
  @throws(classOf[shards.ShardException]) def getForwardingsForTableIds(tableIds: Seq[Int]): Seq[Forwarding]
  @throws(classOf[shards.ShardException]) def shardsForHostname(hostname: String): Seq[ShardInfo]
  @throws(classOf[shards.ShardException]) def listShards(): Seq[ShardInfo]
  @throws(classOf[shards.ShardException]) def listLinks(): Seq[LinkInfo]
  @throws(classOf[shards.ShardException]) def getBusyShards(): Seq[ShardInfo]
  @throws(classOf[shards.ShardException]) def rebuildSchema()
  @throws(classOf[shards.ShardException]) def reload()
  @throws(classOf[shards.ShardException]) def listHostnames(): Seq[String]
  @throws(classOf[shards.ShardException]) def listTables(): Seq[Int]

  @throws(classOf[shards.ShardException]) def currentState(): Seq[NameServerState]

  @throws(classOf[shards.ShardException]) def dumpStructure(tableIds: Seq[Int]) = {
    import TreeUtils._

    lazy val shardsById         = listShards().map(s => s.id -> s).toMap
    lazy val linksByUpId        = mapOfSets(listLinks())(_.upId)
    lazy val forwardingsByTable = mapOfSets(getForwardingsForTableIds(tableIds))(_.tableId)

    def extractor(id: Int) = NameServerState.extractTable(id)(forwardingsByTable)(linksByUpId)(shardsById)

    tableIds.map(extractor)
  }

  // Remote Host Cluster Management

  @throws(classOf[shards.ShardException]) def addRemoteHost(h: Host)
  @throws(classOf[shards.ShardException]) def removeRemoteHost(h: String, p: Int)
  @throws(classOf[shards.ShardException]) def setRemoteHostStatus(h: String, p: Int, s: HostStatus.Value)
  @throws(classOf[shards.ShardException]) def setRemoteClusterStatus(c: String, s: HostStatus.Value)

  @throws(classOf[shards.ShardException]) def getRemoteHost(h: String, p: Int): Host
  @throws(classOf[shards.ShardException]) def listRemoteClusters(): Seq[String]
  @throws(classOf[shards.ShardException]) def listRemoteHosts(): Seq[Host]
  @throws(classOf[shards.ShardException]) def listRemoteHostsInCluster(c: String): Seq[Host]
}
