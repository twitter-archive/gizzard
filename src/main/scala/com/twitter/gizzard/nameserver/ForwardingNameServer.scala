package com.twitter.gizzard.nameserver

import shards._


trait ForwardingNameServer {
  def findCurrentForwarding(serviceId: Int, tableId: Int, id: Long): ShardInfo
  def getShardInfo(shardId: Int): ShardInfo
}

