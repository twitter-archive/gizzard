package com.twitter.gizzard.fake

import shards.ReadWriteShard

class ReadWriteShardAdapter(shard: ReadWriteShard[Shard])
    extends shards.ReadWriteShardAdapter(shard) with Shard {

  def get(key: String) = shard.readOperation(address(key), (x: Shard) => x.get(key))
  def put(key: String, value: String) = shard.writeOperation(address(key), (x: Shard) => x.put(key, value))
  
  private def address(s: String) = {
    Some(shards.Address(0, s.charAt(0).asDigit))
  }
}