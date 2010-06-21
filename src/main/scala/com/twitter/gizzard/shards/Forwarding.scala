package com.twitter.gizzard.shards

case class Forwarding[ConcreteShard <: Shard](address:Address, shard: ConcreteShard)
