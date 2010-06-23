package com.twitter.gizzard.shards

object Shard {
   
}

trait Shard {
  def shardInfo: ShardInfo
  def weight: Int
  def children: Seq[Shard]
  
  def equalsOrContains(other: Shard): Boolean = {
    other == this || children.exists(_.equalsOrContains(other))
  }  
  
  def findChild(id: ShardId): Option[Shard] = {
    if(id == shardInfo.id) {
      Some(this)
    } else {
      children.find(_.findChild(id).isDefined) 
    }
  }
}
