package com.twitter.gizzard.fake

import shards.ReadWriteShard

class ReadWriteShardAdapter(shard: ReadWriteShard[Shard])
  extends shards.ReadWriteShardAdapter(shard) with Shard {

  def get(key: String) = shard.readOperation(row(key), (x: Shard) => x.get(key))
  def put(key: String, value: String) = shard.writeOperation(row(key), (x: Shard) => x.put(key, value))
  
  private def row(s: String):(Int, Long) = {
    (1, s.charAt(0).asDigit)
  }
}