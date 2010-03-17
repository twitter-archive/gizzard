package com.twitter.gizzard.fake

import shards.ShardException


trait Shard extends shards.Shard {
  @throws(classOf[ShardException]) def getName: String
  @throws(classOf[ShardException]) def setName(name: String)
}