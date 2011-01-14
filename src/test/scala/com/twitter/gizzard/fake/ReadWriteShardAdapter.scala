package com.twitter.gizzard.fake

import shards.ReadWriteShard

class ReadWriteShardAdapter(shard: ReadWriteShard[Shard])
  extends shards.ReadWriteShardAdapter(shard) with Shard {

  def getAll(k: String): Seq[Option[String]] = shard.readAllOperation(_.get(k))
  def get(k: String):Option[String] = shard.readOperation(_.get(k))
  def put(k: String, v: String):String = shard.writeOperation(_.put(k, v))
}