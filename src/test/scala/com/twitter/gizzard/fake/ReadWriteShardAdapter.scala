package com.twitter.gizzard.fake

import shards.ReadWriteShard

class ReadWriteShardAdapter(shard: ReadWriteShard[Shard])
  extends shards.ReadWriteShardAdapter(shard) with Shard {

  def get(key: String) = shard.readOperation(_.get(key))
  def put(key: String, value: String) = shard.writeOperation(_.put(key, value))
}