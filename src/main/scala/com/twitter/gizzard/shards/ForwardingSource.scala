package com.twitter.gizzard.shards

trait ForwardingSource {
  @throws(classOf[shards.ShardException]) def getForwardingsForShard(id: ShardId): Seq[nameserver.Forwarding]
  @throws(classOf[shards.ShardException]) def getForwardings(): Seq[nameserver.Forwarding]
  
  def toForwardingTable[ConcreteShard](children: Seq[ConcreteShard]) = {
    val childrenById = mutable.Map.empty[ShardId, ConcreteShard]
    children.foreach { shard => childrenById.put(shard.shardInfo.id, shard) }
    val forwardings = getForwardings.map {orig => Forwarding(Address(orig.tableId, orig.baseId), childrenById.getOrElse(orig.shardId, throw new NonExistentShard) )}
    new ForwardingTable[ConcreteShard](forwardings)
  }
}