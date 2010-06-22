package com.twitter.gizzard.nameserver

import shards._


class ReadWriteShardAdapter(shard: ReadWriteShard[Shard]) extends shards.ReadWriteShardAdapter(shard) with Shard {
  def getBusyShards()                                                           = shard.readOperation(None, _.getBusyShards())
  def getChildShardsOfClass(parentId: ShardId, className: String)               = shard.readOperation(None, _.getChildShardsOfClass(parentId, className))
  def getForwarding(tableId: Int, baseId: Long)                                 = shard.readOperation(None, _.getForwarding(tableId, baseId))
  def getForwardingsForShard(id: ShardId)                                       = shard.readOperation(None, _.getForwardingsForShard(id))
  def getForwardings()                                                          = shard.readOperation(None, _.getForwardings())
  def getShard(id: ShardId)                                                     = shard.readOperation(None, _.getShard(id))
  def listUpwardLinks(id: ShardId)                                              = shard.readOperation(None, _.listUpwardLinks(id))
  def listDownwardLinks(id: ShardId)                                            = shard.readOperation(None, _.listDownwardLinks(id))
  def listLinks()                                                               = shard.readOperation(None, _.listLinks())
  def listShards()                                                              = shard.readOperation(None, _.listShards())
  def shardsForHostname(hostname: String)                                       = shard.readOperation(None, _.shardsForHostname(hostname))

  def createShard[S <: shards.Shard](shardInfo: ShardInfo)                      = shard.writeOperation(None, _.createShard(shardInfo))
  def deleteShard(id: ShardId)                                                  = shard.writeOperation(None, _.deleteShard(id))
  def markShardBusy(id: ShardId, busy: Busy.Value)                              = shard.writeOperation(None, _.markShardBusy(id, busy))
  def addLink(upId: ShardId, downId: ShardId, weight: Int)                      = shard.writeOperation(None, _.addLink(upId, downId, weight))
  def removeLink(upId: ShardId, downId: ShardId)                                = shard.writeOperation(None, _.removeLink(upId, downId))
  def replaceForwarding(oldId: ShardId, newId: ShardId)                         = shard.writeOperation(None, _.replaceForwarding(oldId, newId))
  def setForwarding(forwarding: Forwarding)                                     = shard.writeOperation(None, _.setForwarding(forwarding))

  def reload()                                                                  = shard.readOperation(None, _.reload())
  def rebuildSchema()                                                           = shard.writeOperation(None, _.rebuildSchema())
}
