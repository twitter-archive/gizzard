package com.twitter.gizzard.nameserver

import shards._
import scala.collection.Map


trait Shard extends shards.Shard {
  @throws(classOf[shards.ShardException]) def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S])
  @throws(classOf[shards.ShardException]) def getShard(id: ShardId): ShardInfo
  @throws(classOf[shards.ShardException]) def deleteShard(id: ShardId)
  @throws(classOf[shards.ShardException]) def addLink(upId: ShardId, downId: ShardId, weight: Int)
  @throws(classOf[shards.ShardException]) def removeLink(upId: ShardId, downId: ShardId)
  @throws(classOf[shards.ShardException]) def listUpwardLinks(id: ShardId): Seq[LinkInfo]
  @throws(classOf[shards.ShardException]) def listDownwardLinks(id: ShardId): Seq[LinkInfo]
  @throws(classOf[shards.ShardException]) def markShardBusy(id: ShardId, busy: Busy.Value)
  @throws(classOf[shards.ShardException]) def setForwarding(forwarding: Forwarding)
  @throws(classOf[shards.ShardException]) def replaceForwarding(oldId: ShardId, newId: ShardId)
  @throws(classOf[shards.ShardException]) def getForwarding(tableId: Int, baseId: Long): Forwarding
  @throws(classOf[shards.ShardException]) def getForwardingForShard(id: ShardId): Forwarding
  @throws(classOf[shards.ShardException]) def getForwardings(): Seq[Forwarding]
  @throws(classOf[shards.ShardException]) def shardsForHostname(hostname: String): Seq[ShardInfo]
  @throws(classOf[shards.ShardException]) def listShards(): Seq[ShardInfo]
  @throws(classOf[shards.ShardException]) def listLinks(): Seq[LinkInfo]
  @throws(classOf[shards.ShardException]) def getBusyShards(): Seq[ShardInfo]
  @throws(classOf[shards.ShardException]) def rebuildSchema()
  @throws(classOf[shards.ShardException]) def reload()
  @throws(classOf[shards.ShardException]) def dumpStructure(tableId: Int): NameServerState
  @throws(classOf[shards.ShardException]) def listHostnames(): Seq[String]
  @throws(classOf[shards.ShardException]) def removeForwarding(forwarding: Forwarding)


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
