package com.twitter.gizzard.shards

import nameserver.BasicShardRepository

class DispatchWithoutAddress extends Exception

class DispatchingShardFactory[ConcreteShard <: Shard](val repo: BasicShardRepository) extends ShardFactory[ConcreteShard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, table: ForwardingTable[ConcreteShard]) {
    children = repo.linkSource.listDownwardLinks.map { link => 
      val info = repo.shardInfoSource.getShard(link.downId)
      repo.instantiate(info, link.weight, table.getForwardingsForShard(info).toForwardingTable)
    }
    repo.constructor(new DispatchingShard(shardInfo, weight, table))
  }
  
  def materialize(shardInfo: ShardInfo) = ()
}

class DispatchingShard [ConcreteShard <: Shard](
      val shardInfo: ShardInfo, val weight: Int, table: ForwardingTable[ConcreteShard]
      ) extends ReadWriteShard[ConcreteShard] {
                
  def children = {
    table.shards
  }
  
  def readOperation[A](address: Option[Address], method: (ConcreteShard => A)): A = {
    method(table.findCurrentForwarding(address.getOrElse{ throw new DispatchWithoutAddress }))
  }
  
  def writeOperation[A](address: Option[Address], method: (ConcreteShard => A)): A = {
    method(table.findCurrentForwarding(address.getOrElse{ throw new DispatchWithoutAddress }))
  }
  
}
