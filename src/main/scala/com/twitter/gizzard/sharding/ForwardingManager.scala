package com.twitter.gizzard.sharding


trait ForwardingManager[S <: Shard] {
  //def create(key: Key, shardId: Int)

  //def find(key: Key): S

  def setForwarding(forwarding: Forwarding)

  def replaceForwarding(oldShardId: Int, newShardId: Int)

  def getForwarding(tableId: List[Int], baseId: Long): Int

  def getForwardingForShard(shardId: Int): Forwarding

  def getForwardings(): List[Forwarding]

  def findCurrentForwarding(tableId: List[Int], id: Long): S

  def reloadForwardings()
}
