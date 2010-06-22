package com.twitter.gizzard.shards

trait Forwarder[ConcreteShard <: Shard] {
  def forwardingsForShard(shard: ConcreteShard): Seq[Forwarding[ConcreteShard]]
  def findCurrentForwarding(address: Address): ConcreteShard
  def shards(): Seq[ConcreteShard]
  def forwardings(): Seq[Forwarding[ConcreteShard]]
}