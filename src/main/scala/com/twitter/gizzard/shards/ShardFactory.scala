package com.twitter.gizzard.shards


trait ShardFactory[ConcreteShard <: Shard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, table: ForwardingTable[ConcreteShard]): ConcreteShard
  def materialize(shardInfo: ShardInfo)
  
}

class BasicShardFactory[ConcreteShard <: Shard](repo: BasicShardRepository) extends ShardFactory {
  
  protected def getChildren(info: ShardInfo, table: ForwardingTable) {
    repo.linkSource.listDownwardLinks(info.id).map { link => 
      val info = repo.shardInfoSource.getShard(link.downId)
      repo.instantiate(info, link.weight, table)
    }
  }
}