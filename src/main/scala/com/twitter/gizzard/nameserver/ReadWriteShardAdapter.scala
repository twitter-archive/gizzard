package com.twitter.gizzard
package nameserver

import shards.{RoutingNode, ShardId, ShardInfo, Busy}

class ReadWriteShardAdapter(node: RoutingNode[Shard]) extends Shard {
  def getBusyShards()                                             = node.readOperation(_.getBusyShards())
  def getForwarding(tableId: Int, baseId: Long)                   = node.readOperation(_.getForwarding(tableId, baseId))
  def getForwardingForShard(id: ShardId)                          = node.readOperation(_.getForwardingForShard(id))
  def getForwardings()                                            = node.readOperation(_.getForwardings())
  def getForwardingsForTableIds(tableIds: Seq[Int]): Seq[Forwarding] = node.readOperation(_.getForwardingsForTableIds(tableIds))
  def getShard(id: ShardId)                                       = node.readOperation(_.getShard(id))
  def listUpwardLinks(id: ShardId)                                = node.readOperation(_.listUpwardLinks(id))
  def listDownwardLinks(id: ShardId)                              = node.readOperation(_.listDownwardLinks(id))
  def listLinks()                                                 = node.readOperation(_.listLinks())
  def listShards()                                                = node.readOperation(_.listShards())
  def shardsForHostname(hostname: String)                         = node.readOperation(_.shardsForHostname(hostname))
  def listHostnames()                                             = node.readOperation(_.listHostnames())
  def listTables()                                                = node.readOperation(_.listTables())

  def currentState()                                              = node.readOperation(_.currentState())

  def createShard[T](shardInfo: ShardInfo, repository: ShardRepository[T]) = node.writeOperation(_.createShard(shardInfo, repository))
  def deleteShard(id: ShardId)                                    = node.writeOperation(_.deleteShard(id))
  def markShardBusy(id: ShardId, busy: Busy.Value)                = node.writeOperation(_.markShardBusy(id, busy))
  def addLink(upId: ShardId, downId: ShardId, weight: Int)        = node.writeOperation(_.addLink(upId, downId, weight))
  def removeLink(upId: ShardId, downId: ShardId)                  = node.writeOperation(_.removeLink(upId, downId))
  def replaceForwarding(oldId: ShardId, newId: ShardId)           = node.writeOperation(_.replaceForwarding(oldId, newId))
  def setForwarding(forwarding: Forwarding)                       = node.writeOperation(_.setForwarding(forwarding))
  def removeForwarding(forwarding: Forwarding)                    = node.writeOperation(_.removeForwarding(forwarding))
  def reload()                                                    = node.writeOperation(_.reload())
  def rebuildSchema()                                             = node.writeOperation(_.rebuildSchema())


  // Remote Host Cluster Management

  def addRemoteHost(h: Host)                                      = node.writeOperation(_.addRemoteHost(h))
  def removeRemoteHost(h: String, p: Int)                         = node.writeOperation(_.removeRemoteHost(h, p))
  def setRemoteHostStatus(h: String, p: Int, s: HostStatus.Value) = node.writeOperation(_.setRemoteHostStatus(h, p, s))
  def setRemoteClusterStatus(c: String, s: HostStatus.Value)      = node.writeOperation(_.setRemoteClusterStatus(c, s))

  def getRemoteHost(h: String, p: Int)                            = node.readOperation(_.getRemoteHost(h, p))
  def listRemoteClusters()                                        = node.readOperation(_.listRemoteClusters())
  def listRemoteHosts()                                           = node.readOperation(_.listRemoteHosts())
  def listRemoteHostsInCluster(c: String)                         = node.readOperation(_.listRemoteHostsInCluster(c))
}
