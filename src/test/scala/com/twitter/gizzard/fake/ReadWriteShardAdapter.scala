package com.twitter.gizzard
package fake

class ReadWriteShardAdapter(shard: shards.ReadWriteShard[Shard])
  extends shards.ReadWriteShardAdapter(shard) with Shard {

  def getAll(k: String) = shard.readAllOperation(_.get(k))
  def get(k: String) = shard.readOperation(_.get(k))
  def put(k: String, v: String) = shard.writeOperation(_.put(k, v))
}
