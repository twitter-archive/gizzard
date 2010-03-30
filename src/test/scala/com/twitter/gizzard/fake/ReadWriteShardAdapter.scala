package com.twitter.gizzard.fake

import shards.ReadWriteShard


class ReadWriteShardAdapter(shard: ReadWriteShard[Shard])
  extends shards.ReadWriteShardAdapter(shard) with Shard {

  def getName() = shard.readOperation(_.getName)
  def setName(name: String) = shard.writeOperation(_.setName(name))
}