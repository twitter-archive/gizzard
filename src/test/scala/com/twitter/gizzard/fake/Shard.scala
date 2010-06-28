package com.twitter.gizzard.fake

import shards.ShardException


trait Shard extends shards.Shard {
  @throws(classOf[ShardException]) def get(k: String): Option[String]
  @throws(classOf[ShardException]) def put(k: String, v: String):String
}