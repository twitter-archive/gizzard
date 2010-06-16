package com.twitter.gizzard.fake

import shards.ReadWriteShard

class ReadWriteShardAdapter(shard: ReadWriteShard[Shard])
  extends shards.ReadWriteShardAdapter(shard) with Shard {

  def get(key: String) = shard.readOperation(offset(key), _.get(key))
  def put(key: String, value: String) = shard.writeOperation(offset(key), _.put(key, value))
  
  private def offset(s: String):Long = {
    s.charAt(0).asDigit
  }
}