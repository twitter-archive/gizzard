package com.twitter.gizzard.nameserver

import com.twitter.gizzard.shards._


class ShardManager(shard: RoutingNode[ShardManagerSource]) {
  def dumpStructure(tableIds: Seq[Int]) = shard.read.any(_.dumpStructure(tableIds))

  def createShard(shardInfo: ShardInfo)            { shard.write.foreach(_.createShard(shardInfo)) }
  def deleteShard(id: ShardId)                     { shard.write.foreach(_.deleteShard(id)) }
  def markShardBusy(id: ShardId, busy: Busy.Value) { shard.write.foreach(_.markShardBusy(id, busy)) }

  // XXX: removing this causes CopyJobSpec to fail.
  @throws(classOf[NonExistentShard])
  def getShard(id: ShardId)               = shard.read.any(_.getShard(id))
  def shardsForHostname(hostname: String) = shard.read.any(_.shardsForHostname(hostname))
  def listShards()                        = shard.read.any(_.listShards())
  def getBusyShards()                     = shard.read.any(_.getBusyShards())


  def addLink(upId: ShardId, downId: ShardId, weight: Int) { shard.write.foreach(_.addLink(upId, downId, weight)) }
  def removeLink(upId: ShardId, downId: ShardId)           { shard.write.foreach(_.removeLink(upId, downId)) }

  def listUpwardLinks(id: ShardId)   = shard.read.any(_.listUpwardLinks(id))
  def listDownwardLinks(id: ShardId) = shard.read.any(_.listDownwardLinks(id))
  def listLinks()                    = shard.read.any(_.listLinks())


  def setForwarding(forwarding: Forwarding)             { shard.write.foreach(_.setForwarding(forwarding)) }
  def removeForwarding(f: Forwarding)                   { shard.write.foreach(_.removeForwarding(f)) }
  def replaceForwarding(oldId: ShardId, newId: ShardId) { shard.write.foreach(_.replaceForwarding(oldId, newId)) }

  def getForwarding(tableId: Int, baseId: Long) = shard.read.any(_.getForwarding(tableId, baseId))
  def getForwardingForShard(id: ShardId)        = shard.read.any(_.getForwardingForShard(id))
  def getForwardings()                          = shard.read.any(_.getForwardings())


  def listHostnames() = shard.read.any(_.listHostnames())
  def listTables()    = shard.read.any(_.listTables())
}
