package com.twitter.gizzard.fake

import shards.ShardException
import org.specs.mock.{ClassMocker, JMocker}

trait Shard extends shards.Shard {
  @throws(classOf[ShardException]) def get(key: String): Option[String]
  @throws(classOf[ShardException]) def put(key: String, value: String)
}