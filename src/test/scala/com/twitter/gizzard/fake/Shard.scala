package com.twitter.gizzard.fake

import shards.ShardException
import org.specs.mock.{ClassMocker, JMocker}

trait Shard extends shards.Shard {
  @throws(classOf[ShardException]) def getName(): String
  @throws(classOf[ShardException]) def setName(name: String)
}