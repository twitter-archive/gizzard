package com.twitter.gizzard.shards

class DispatchWithoutAddress extends Exception

class DispatchingShard [ConcreteShard <: Shard](
      val shardInfo: ShardInfo, val weight: Int, val forwardingTable: ForwardingTable[ConcreteShard]
      ) extends ReadWriteShard[ConcreteShard] {
        
  def children = {
    forwardingTable.getShards
  }
  
  def readOperation[A](address: Option[Address], method: (ConcreteShard => A)): A = {
    method(forwardingTable.getShard(address.getOrElse{ throw new DispatchWithoutAddress }))
  }
  
  def writeOperation[A](address: Option[Address], method: (ConcreteShard => A)): A = {
    method(forwardingTable.getShard(address.getOrElse{ throw new DispatchWithoutAddress }))
  }
  
}
