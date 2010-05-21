package com.twitter.gizzard.nameserver

import shards._
import scala.collection.Map


trait Shard extends shards.Shard {
  @throws(classOf[InvalidShard])
  def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S])
  def getShard(id: ShardId): ShardInfo
  def deleteShard(id: ShardId)
  def addLink(upId: ShardId, downId: ShardId, weight: Int)
  def removeLink(upId: ShardId, downId: ShardId)
  def listUpwardLinks(id: ShardId): Seq[LinkInfo]
  def listDownwardLinks(id: ShardId): Seq[LinkInfo]
  def markShardBusy(id: ShardId, busy: Busy.Value)
  def setForwarding(forwarding: Forwarding)
  def replaceForwarding(oldId: ShardId, newId: ShardId)
  def getForwarding(tableId: Int, baseId: Long): Forwarding
  def getForwardingForShard(id: ShardId): Forwarding
  def getForwardings(): Seq[Forwarding]
  def shardsForHostname(hostname: String): Seq[ShardInfo]
  def listShards(): Seq[ShardInfo]
  def listLinks(): Seq[LinkInfo]
  def getBusyShards(): Seq[ShardInfo]
  def getChildShardsOfClass(parentId: ShardId, className: String): Seq[ShardInfo]
  def rebuildSchema()
  def reload()
}
