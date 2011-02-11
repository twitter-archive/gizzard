package com.twitter.gizzard.fake

import shards.ShardException


trait Shard extends shards.Shard {
  @throws(classOf[Throwable]) def get(k: String): Option[String]
  @throws(classOf[Throwable]) def put(k: String, v: String):String
}
