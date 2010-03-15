package com.twitter.gizzard.nameserver

import shards._


trait ReadWriteNameServer[S <: Shard] extends NameServer[S] with ReadWriteShard[NameServer[S]] {
  def findCurrentForwarding(tableId: List[Int], id: Long)                       = readOperation(_.findCurrentForwarding(tableId, id))
  def findShard(shardInfo: ShardInfo)                                           = readOperation(_.findShard(shardInfo))
  def getBusyShards()                                                           = readOperation(_.getBusyShards())
  def getChildShardsOfClass(parentShardId: Int, className: String)              = readOperation(_.getChildShardsOfClass(parentShardId, className))
  def getForwarding(tableId: List[Int], baseId: Long)                           = readOperation(_.getForwarding(tableId, baseId))
  def getForwardingForShard(shardId: Int)                                       = readOperation(_.getForwardingForShard(shardId))
  def getForwardings()                                                          = readOperation(_.getForwardings())
  def getParentShard(shardId: Int)                                              = readOperation(_.getParentShard(shardId))
  def getRootShard(shardId: Int)                                                = readOperation(_.getRootShard(shardId))
  def getShard(shardId: Int)                                                    = readOperation(_.getShard(shardId))
  def listShardChildren(shardId: Int)                                           = readOperation(_.listShardChildren(shardId))
  def shardIdsForHostname(hostname: String, className: String): List[Int]       = readOperation(_.shardIdsForHostname(hostname, className))
  def shardsForHostname(hostname: String, className: String): List[ShardInfo]   = readOperation(_.shardsForHostname(hostname, className))

  def addChildShard(parentShardId: Int, childShardId: Int, weight: Int)         = writeOperation(_.addChildShard(parentShardId, childShardId, weight))
  def createShard(shardInfo: ShardInfo)                                         = writeOperation(_.createShard(shardInfo))
  def deleteShard(shardId: Int)                                                 = writeOperation(_.deleteShard(shardId))
  def markShardBusy(shardId: Int, busy: Busy.Value)                             = writeOperation(_.markShardBusy(shardId, busy))
  def removeChildShard(parentShardId: Int, childShardId: Int)                   = writeOperation(_.removeChildShard(parentShardId, childShardId))
  def replaceChildShard(oldChildShardId: Int, newChildShardId: Int)             = writeOperation(_.replaceChildShard(oldChildShardId, newChildShardId))
  def replaceForwarding(oldShardId: Int, newShardId: Int)                       = writeOperation(_.replaceForwarding(oldShardId, newShardId))
  def setForwarding(forwarding: Forwarding)                                     = writeOperation(_.setForwarding(forwarding))
  def updateShard(shardInfo: ShardInfo)                                         = writeOperation(_.updateShard(shardInfo))

  def rebuildSchema() = readOperation(_.rebuildSchema())
  def findShardById(shardId: Int, weight: Int) = readOperation(_.findShardById(shardId, weight))
  def reload() = readOperation(_.reload())
  def reloadForwardings() = readOperation(_.reloadForwardings())
  def finishMigration(migration: ShardMigration) = readOperation(_.finishMigration(migration)) // wtf
  def migrateShard(migration: ShardMigration) = readOperation(_.migrateShard(migration)) // wtf
  def setupMigration(sourceShardInfo: ShardInfo, destinationShardInfo: ShardInfo) = readOperation(_.setupMigration(sourceShardInfo, destinationShardInfo))
  def copyShard(sourceShardId: Int, destinationShardId: Int) = readOperation(_.copyShard(sourceShardId, destinationShardId))
}
