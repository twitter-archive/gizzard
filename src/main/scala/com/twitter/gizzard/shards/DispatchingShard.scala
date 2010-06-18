package com.twitter.gizzard.shards

trait DispatchingShard[ConcreteShard <: Shard] extends ReadWriteShard[ConcreteShard] {

  override def readOperation[A](address: Option[(Int, Long)], method: (ConcreteShard => A)): A
  override def writeOperation[A](address: Option[(Int, Long)], method: (ConcreteShard => A)): A 

  def findCurrentForwarding(address: (Int, Long)): ConcreteShard
  
  def getForwarding(tableId: Int, baseId: Long): Forwarding
  
  def getForwardingsForShard(id: ShardId): Seq[Forwarding]
  
    def getForwardings(): Seq[Forwarding]
  
    // extract....
    @throws(classOf[NonExistentShard])      def findShardById(id: ShardId): S
                                            def findCurrentForwarding(address: (Int, Long)): S
  }
}
