package com.twitter.gizzard.shards
import scala.collection.mutable

class ShardTreeFactory[ConcreteShard <: Shard](
      val shardFactory: ShardFactory[ConcreteShard], val linkSource: LinkSource, val shardInfoSource: ShardInfoSource) {

  val linksById = mutable.Map.empty[ShardId, mutable.ListBuffer[LinkInfo]]
  val shardInfosById = mutable.Map.empty[ShardId, ShardInfo]
  
  linkSource.listLinks.foreach { link => linksById.getOrElseUpdate(link.upId, new mutable.ListBuffer).append(link) }
  shardInfoSource.listShards.foreach { shard => shardInfosById.put(shard.id, shard) }
        
  def instantiate(root: ShardId): ConcreteShard = instantiate(root, 1)  
  
  def instantiate(id: ShardId, weight: Int): ConcreteShard = {
    val children = linksById.getOrElse(id, Seq()).map(link => instantiate(link.downId, link.weight))
    shardFactory.instantiate(shardInfosById.getOrElse(id, throw new NonExistentShard), weight, children)
  }
}