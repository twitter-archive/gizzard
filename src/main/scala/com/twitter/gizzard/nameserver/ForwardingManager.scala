package com.twitter.gizzard.nameserver

import shards.Shard


trait ForwardingManager[S <: Shard] {
  def setForwarding(forwarding: Forwarding)

  def replaceForwarding(oldShardId: Int, newShardId: Int)

  def getForwarding(tableId: List[Int], baseId: Long): Int

  def getForwardingForShard(shardId: Int): Forwarding

  def getForwardings(): List[Forwarding]

  def findCurrentForwarding(tableId: List[Int], id: Long): S

  def reloadForwardings(nameServer: NameServer[S])
}
