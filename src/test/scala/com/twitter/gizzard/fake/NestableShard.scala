package com.twitter.gizzard.fake

import shards.ShardException
import org.specs.mock.{ClassMocker, JMocker}
import scala.collection.mutable

class NestableShard(val children: Seq[Shard]) extends Shard {
  val weight = 1
  val shardInfo = new ShardInfo
  val map = new mutable.HashMap[String, String]
  
  def get(key: String) = {
    map.get(key)
  }
  
  def put(key: String, value: String) = {
    map.put(key, value)
  }
  
}