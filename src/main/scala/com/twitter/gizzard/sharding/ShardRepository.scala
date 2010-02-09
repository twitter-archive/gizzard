package com.twitter.gizzard.sharding

import scala.collection.mutable
import gen.ShardInfo


class ShardRepository[S <: Shard] {
  private val shardFactories = mutable.Map.empty[String, ShardFactory[S]]

  def +=(item: (String, ShardFactory[S])) {
    shardFactories += item
  }

  def find(shardInfo: ShardInfo, weight: Int, children: Seq[S]) = {
    shardFactories(shardInfo.class_name).instantiate(shardInfo, weight, children)
  }

  def create(shardInfo: ShardInfo) {
    shardFactories(shardInfo.class_name).materialize(shardInfo)
  }
}
