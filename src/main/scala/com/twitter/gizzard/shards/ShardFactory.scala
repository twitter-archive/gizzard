package com.twitter.gizzard.shards

import nameserver.NameServer

trait ShardFactory[ConcreteShard <: Shard] {
  def instantiate(nameServer: NameServer[ConcreteShard], shardInfo: ShardInfo, weight: Int, children: Seq[ConcreteShard]): ConcreteShard
  def materialize(shardInfo: ShardInfo)
}
