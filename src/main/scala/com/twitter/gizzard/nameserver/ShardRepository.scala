package com.twitter.gizzard.nameserver

import scala.collection.mutable
import net.lag.logging.ThrottledLogger
import shards.{ShardInfo, ShardFactory}


class ShardRepository[S <: shards.Shard] {
  private val shardFactories = mutable.Map.empty[String, ShardFactory[S]]

  def +=(item: (String, ShardFactory[S])) {
    shardFactories += item
  }

  def find(nameServer: NameServer[S], shardInfo: ShardInfo, weight: Int, children: Seq[S]) = {
    shardFactories(shardInfo.className).instantiate(nameServer, shardInfo, weight, children)
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

  setupPackage("com.twitter.gizzard.shards")

  def setupPackage(packageName: String) {
    this += (packageName + ".ReadOnlyShard"    -> new shards.ReadOnlyShardFactory(constructor))
    this += (packageName + ".BlockedShard"     -> new shards.BlockedShardFactory(constructor))
    this += (packageName + ".WriteOnlyShard"   -> new shards.WriteOnlyShardFactory(constructor))
    this += (packageName + ".ReplicatingShard" ->
             new shards.ReplicatingShardFactory(constructor, log, future))
  }
}
