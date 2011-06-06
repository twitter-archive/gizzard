package com.twitter.gizzard.fake

import scala.collection.mutable
import org.specs.mock.{ClassMocker, JMocker}

import com.twitter.gizzard.shards.{ShardFactory,ShardException,ShardInfo}

class NestableShardFactory extends ShardFactory[Shard] {
  def instantiate(shardInfo: ShardInfo, weight: Int) = {
    new NestableShard(shardInfo)
  }

  def materialize(shardInfo: ShardInfo) = ()
}

class NestableShard(val shardInfo: ShardInfo) extends Shard {
  val map = new mutable.HashMap[String, String]

  def get(key: String) = {
    map.get(key)
  }

  def put(key: String, value: String) = {
    map.put(key, value)
    value
  }
}
