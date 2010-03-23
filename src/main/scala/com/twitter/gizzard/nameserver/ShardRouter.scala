package com.twitter.gizzard.nameserver

import shards._


class ShardRouter[S <: Shard, T](nameServer: ShardNameServer[S], translate: (T => Int)) {
  def apply(sourceId: Long, description: T): S = {
    nameServer.findShardById(nameServer.findCurrentForwarding(translate(description), sourceId).shardId)
  }
}
