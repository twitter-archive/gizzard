package com.twitter.gizzard.nameserver

import scala.collection.mutable
import net.lag.logging.ThrottledLogger
import shards.{ShardInfo, ShardFactory}


class ShardRepository[S <: shards.Shard] {
  private val shardFactories = mutable.Map.empty[String, ShardFactory[S]]

  def +=(item: (String, ShardFactory[S])) {
    shardFactories += item
  }

  def find(shardInfo: ShardInfo, weight: Int, children: Seq[S]) = {
    shardFactories(shardInfo.className).instantiate(shardInfo, weight, children)
  }

  def create(shardInfo: ShardInfo) {
    shardFactories(shardInfo.className).materialize(shardInfo)
  }

  override def toString() = {
    "ShardRepository(" + shardFactories.toString + ")"
  }
}

/**
 * A ShardRepository that is pre-seeded with read-only, write-only, replicating, and blocked
 * shard types.
 */
class BasicShardRepository[S <: shards.Shard](constructor: shards.ReadWriteShard[S] => S,
                                              log: ThrottledLogger[String], future: Future)
      extends ShardRepository[S] {
  this += ("com.twitter.gizzard.shards.ReadOnlyShard"    -> new shards.ReadOnlyShardFactory(constructor))
  this += ("com.twitter.gizzard.shards.BlockedShard"     -> new shards.BlockedShardFactory(constructor))
  this += ("com.twitter.gizzard.shards.WriteOnlyShard"   -> new shards.WriteOnlyShardFactory(constructor))
  this += ("com.twitter.gizzard.shards.ReplicatingShard" ->
           new shards.ReplicatingShardFactory(constructor, log, future))
}
