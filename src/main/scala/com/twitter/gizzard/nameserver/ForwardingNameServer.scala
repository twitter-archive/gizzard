package com.twitter.gizzard.nameserver

import shards._


trait ForwardingNameServer {
  def findCurrentForwarding(tableId: List[Int], id: Long): ShardInfo
  def getShardInfo(shardId: Int): ShardInfo
//  def findShardById(shardId: Int, weight: Int): ShardInfo
//  def findShardById(shardId: Int): ShardInfo = findShardById(shardId, 1)
}

