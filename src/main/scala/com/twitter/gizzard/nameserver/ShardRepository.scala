package com.twitter.gizzard.nameserver

import scala.collection.mutable
import net.lag.logging.ThrottledLogger
import shards.{ShardInfo, ShardFactory, LinkInfo, LinkSource, ForwardingTable, ShardInfoSource, ForwardingSource}


class ShardRepository[S <: shards.Shard] extends ShardFactory[S] {
  private val shardFactories = mutable.Map.empty[String, ShardFactory[S]]

  def +=(item: (String, ShardFactory[S])) {
    shardFactories += item
  }
  
  def instantiate(shardInfo: ShardInfo, weight: Int, forwarding: ForwardingTable[ConcreteShard]) = {
    shardFactories(shardInfo.className).instantiate(shardInfo, weight, forwarding)
  }

  def materialize(shardInfo: ShardInfo) {
    shardFactories(shardInfo.className).materialize(shardInfo)
  }

  override def toString() = {
    "ShardRepository(" + shardFactories.toString + ")"
  }
}

/**
 * A ShardRepository that is pre-seeded
 */
class BasicShardRepository[S <: shards.Shard](val constructor: shards.ReadWriteShard[S] => S,
      val linkSource: LinkSource, val shardInfoSource: ShardInfoSource, val forwardingSource: ForwardingSource,
      val log: ThrottledLogger[String], val future: Future)
      extends ShardRepository[S] {

  setupPackage("com.twitter.gizzard.shards")
    
  def setupPackage(packageName: String) {
    this += (packageName + ".DispatchingShard"  -> new shards.DispatchingShardFactory(this))
    this += (packageName + ".ReadOnlyShard"     -> new shards.ReadOnlyShardFactory(this))
    this += (packageName + ".BlockedShard"      -> new shards.BlockedShardFactory(this))
    this += (packageName + ".WriteOnlyShard"    -> new shards.WriteOnlyShardFactory(this))
    this += (packageName + ".ReplicatingShard"  -> new shards.ReplicatingShardFactory(this))
  }
}
