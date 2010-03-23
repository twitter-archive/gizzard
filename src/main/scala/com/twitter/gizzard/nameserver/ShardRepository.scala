package com.twitter.gizzard.nameserver

import scala.collection.mutable
import shards.{Shard, ShardInfo, ShardFactory}

class ShardRepository[S <: Shard] {
  private val shardFactories = mutable.Map.empty[String, ShardFactory[S]]

  def +=(item: (String, ShardFactory[S])) {
    shardFactories += item
  }

  def find(shardInfo: ShardInfo, weight: Int, children: Seq[S]) = {
    shardFactories(shardInfo.className).instantiate(shardInfo, weight, children)
  }

  override def toString() = {
    "ShardRepository(" + shardFactories.toString + ")"
  }
}
