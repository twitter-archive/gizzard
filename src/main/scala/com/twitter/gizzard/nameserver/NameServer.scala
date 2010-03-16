package com.twitter.gizzard.nameserver

import shards._

trait NameServer[S <: Shard] extends Shard {
  def createShard(shardInfo: ShardInfo): Int
  def findShard(shardInfo: ShardInfo): Int
  def getShard(shardId: Int): ShardInfo
  def updateShard(shardInfo: ShardInfo)
  def deleteShard(shardId: Int)
  def addChildShard(parentShardId: Int, childShardId: Int, weight: Int)
  def removeChildShard(parentShardId: Int, childShardId: Int)
  def replaceChildShard(oldChildShardId: Int, newChildShardId: Int)
  def listShardChildren(shardId: Int): Seq[ChildInfo]
  def markShardBusy(shardId: Int, busy: Busy.Value)
  def setForwarding(forwarding: Forwarding)
  def replaceForwarding(oldShardId: Int, newShardId: Int)
  def getForwarding(tableId: List[Int], baseId: Long): ShardInfo
  def getForwardingForShard(shardId: Int): Forwarding
  def getForwardings(): List[Forwarding]
  def findCurrentForwarding(tableId: List[Int], id: Long): S
  def reloadForwardings()
  def shardIdsForHostname(hostname: String, className: String): List[Int]
  def shardsForHostname(hostname: String, className: String): List[ShardInfo]
  def getBusyShards(): Seq[ShardInfo]
  def getParentShard(shardId: Int): ShardInfo
  def getRootShard(shardId: Int): ShardInfo
  def getChildShardsOfClass(parentShardId: Int, className: String): List[ShardInfo]
  def reload()
  def findShardById(shardId: Int, weight: Int): S
  def findShardById(shardId: Int): S = findShardById(shardId, 1)
  def rebuildSchema()
}

class NonExistentShard extends ShardException("Shard does not exist")
class InvalidShard extends ShardException("Shard has invalid attributes (such as hostname)")
