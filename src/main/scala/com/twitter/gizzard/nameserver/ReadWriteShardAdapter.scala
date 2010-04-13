package com.twitter.gizzard.nameserver

import shards._


class ReadWriteShardAdapter(shard: ReadWriteShard[Shard]) extends shards.ReadWriteShardAdapter(shard) with Shard {
  def findShard(shardInfo: ShardInfo)                                           = shard.readOperation(_.findShard(shardInfo))
  def getBusyShards()                                                           = shard.readOperation(_.getBusyShards())
  def getChildShardsOfClass(parentShardId: Int, className: String)              = shard.readOperation(_.getChildShardsOfClass(parentShardId, className))
  def getForwarding(tableId: Int, baseId: Long)                                 = shard.readOperation(_.getForwarding(tableId, baseId))
  def getForwardingForShard(shardId: Int)                                       = shard.readOperation(_.getForwardingForShard(shardId))
  def getForwardings()                                                          = shard.readOperation(_.getForwardings())
  def getParentShard(shardId: Int)                                              = shard.readOperation(_.getParentShard(shardId))
  def getRootShard(shardId: Int)                                                = shard.readOperation(_.getRootShard(shardId))
  def getShard(shardId: Int)                                                    = shard.readOperation(_.getShard(shardId))
  def listShardChildren(parentId: Int)                                          = shard.readOperation(_.listShardChildren(parentId))
  def listShardChildren()                                                       = shard.readOperation(_.listShardChildren())
  def listShards()                                                              = shard.readOperation(_.listShards())
  def shardIdsForHostname(hostname: String, className: String): List[Int]       = shard.readOperation(_.shardIdsForHostname(hostname, className))
  def shardsForHostname(hostname: String, className: String): List[ShardInfo]   = shard.readOperation(_.shardsForHostname(hostname, className))

  def addChildShard(parentShardId: Int, childShardId: Int, weight: Int)         = shard.writeOperation(_.addChildShard(parentShardId, childShardId, weight))
  def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S]) = shard.writeOperation(_.createShard(shardInfo, repository))
  def deleteShard(shardId: Int)                                                 = shard.writeOperation(_.deleteShard(shardId))
  def markShardBusy(shardId: Int, busy: Busy.Value)                             = shard.writeOperation(_.markShardBusy(shardId, busy))
  def removeChildShard(parentShardId: Int, childShardId: Int)                   = shard.writeOperation(_.removeChildShard(parentShardId, childShardId))
  def replaceChildShard(oldChildShardId: Int, newChildShardId: Int)             = shard.writeOperation(_.replaceChildShard(oldChildShardId, newChildShardId))
  def replaceForwarding(oldShardId: Int, newShardId: Int)                       = shard.writeOperation(_.replaceForwarding(oldShardId, newShardId))
  def setForwarding(forwarding: Forwarding)                                     = shard.writeOperation(_.setForwarding(forwarding))
  def updateShard(shardInfo: ShardInfo)                                         = shard.writeOperation(_.updateShard(shardInfo))

  def reload()                                                                  = shard.readOperation(_.reload())
  def rebuildSchema()                                                           = shard.writeOperation(_.rebuildSchema())
}
