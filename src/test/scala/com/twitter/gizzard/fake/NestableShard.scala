package com.twitter.gizzard.fake

import scala.collection.mutable
import org.specs.mock.{ClassMocker, JMocker}

import com.twitter.gizzard.shards.{ShardFactory,ShardException,ShardInfo,Weight}

class NestableShardFactory extends ShardFactory[Shard] {
  def instantiate(shardInfo: ShardInfo, weight: Weight) = {
    new NestableShard(shardInfo, false)
  }

  def instantiateReadOnly(shardInfo: ShardInfo, weight: Weight) = {
    new NestableShard(shardInfo, true)
  }

  def materialize(shardInfo: ShardInfo) = ()
}

class NestableShard(val shardInfo: ShardInfo, readOnly: Boolean) extends Shard {
  val map = new mutable.HashMap[String, String]

  def get(key: String) = {
    map.get(key)
  }

  def put(key: String, value: String) = {
    if (readOnly) error("shard is read only!")
    map.put(key, value)
    value
  }
}
