package com.twitter.gizzard.nameserver

import shards._
import scala.collection.Map


trait Shard extends shards.Shard {
  def createShard(shardInfo: ShardInfo, materialize: => Unit): Int
  def findShard(shardInfo: ShardInfo): Int
  def getShard(shardId: Int): ShardInfo
  def updateShard(shardInfo: ShardInfo)
  def deleteShard(shardId: Int)
  def addChildShard(parentShardId: Int, childShardId: Int, weight: Int)
  def removeChildShard(parentShardId: Int, childShardId: Int)
  def replaceChildShard(oldChildShardId: Int, newChildShardId: Int)
  def markShardBusy(shardId: Int, busy: Busy.Value)
  def setForwarding(forwarding: Forwarding)
  def replaceForwarding(oldShardId: Int, newShardId: Int)
  def getForwarding(tableId: Int, baseId: Long): ShardInfo
  def getForwardingForShard(shardId: Int): Forwarding
  def getForwardings(): Seq[Forwarding]
  def shardIdsForHostname(hostname: String, className: String): List[Int]
  def listShards(): Seq[ShardInfo]
  def listShardChildren(): Map[Int, Seq[ChildInfo]]
  def shardsForHostname(hostname: String, className: String): List[ShardInfo]
  def getBusyShards(): Seq[ShardInfo]
  def getParentShard(shardId: Int): ShardInfo
  def getRootShard(shardId: Int): ShardInfo
  def getChildShardsOfClass(parentShardId: Int, className: String): List[ShardInfo]
  def rebuildSchema()
  def reload()
  def listShardChildren(parentId: Int): Seq[ChildInfo]
}
