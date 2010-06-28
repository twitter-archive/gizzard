package com.twitter.gizzard.fake

import net.lag.logging.ThrottledLogger
import shards.{ShardInfo, ShardFactory}

/**
 * A ShardRepository that is pre-seeded with read-only, write-only, replicating, and blocked
 * shard types.
 */
class ShardRepository[S <: shards.Shard](constructor: shards.ReadWriteShard[S] => S,
                                              log: ThrottledLogger[String], future: Future)
      extends shards.ShardRepository[S] {
    
  this += "com.twitter.gizzard.fake.NestableShard" -> new shards.NestableShardFactory(constructor)
}
