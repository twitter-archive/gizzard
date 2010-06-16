package com.twitter.gizzard.nameserver

import shards._


class ReadWriteShardAdapter(shard: ReadWriteShard[Shard]) extends shards.ReadWriteShardAdapter(shard) with Shard {
  def getBusyShards()                                                           = shard.readOperation(-1, _.getBusyShards())
  def getChildShardsOfClass(parentId: ShardId, className: String)               = shard.readOperation(-1, _.getChildShardsOfClass(parentId, className))
  def getForwarding(tableId: Int, baseId: Long)                                 = shard.readOperation(-1, _.getForwarding(tableId, baseId))
  def getForwardingForShard(id: ShardId)                                        = shard.readOperation(-1, _.getForwardingForShard(id))
  def getForwardings()                                                          = shard.readOperation(-1, _.getForwardings())
  def getShard(id: ShardId)                                                     = shard.readOperation(hash(id), _.getShard(id))
  def listUpwardLinks(id: ShardId)                                              = shard.readOperation(hash(id), _.listUpwardLinks(id))
  def listDownwardLinks(id: ShardId)                                            = shard.readOperation(hash(id), _.listDownwardLinks(id))
  def listLinks()                                                               = shard.readOperation(-1, _.listLinks())
  def listShards()                                                              = shard.readOperation(-1, _.listShards())
  def shardsForHostname(hostname: String)                                       = shard.readOperation(-1, _.shardsForHostname(hostname))

  def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S]) = shard.writeOperation(-1, _.createShard(shardInfo, repository))
  def deleteShard(id: ShardId)                                                  = shard.writeOperation(hash(id), _.deleteShard(id))
  def markShardBusy(id: ShardId, busy: Busy.Value)                              = shard.writeOperation(hash(id), _.markShardBusy(id, busy))
  def addLink(upId: ShardId, downId: ShardId, weight: Int)                      = shard.writeOperation(hash(upId), _.addLink(upId, downId, weight))
  def removeLink(upId: ShardId, downId: ShardId)                                = shard.writeOperation(hash(upId), _.removeLink(upId, downId))
  def replaceForwarding(oldId: ShardId, newId: ShardId)                         = shard.writeOperation(hash(newId), _.replaceForwarding(oldId, newId))
  def setForwarding(forwarding: Forwarding)                                     = shard.writeOperation(-1, _.setForwarding(forwarding))

  def reload()                                                                  = shard.readOperation(-1, _.reload())
  def rebuildSchema()                                                           = shard.writeOperation(-1, _.rebuildSchema())
  
  private def hash(sid: ShardId):Long = {
    sid.hostname.hashCode + sid.tablePrefix.hashCode
  }
}
