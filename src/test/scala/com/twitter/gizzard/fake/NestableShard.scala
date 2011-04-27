package com.twitter.gizzard
package fake

import scala.collection.mutable
import org.specs.mock.{ClassMocker, JMocker}
import shards.{ShardException,ShardInfo}

class NestableShardFactory extends shards.ShardFactory[Shard] {
  def instantiate(shardInfo: ShardInfo) = new NestableShard(shardInfo)
  def materialize(shardInfo: ShardInfo) = ()
}

class NestableShard(val shardInfo: shards.ShardInfo) extends Shard {
  val map = new mutable.HashMap[String, String]

  def get(key: String) = {
    map.get(key)
  }

  def put(key: String, value: String) = {
    map.put(key, value)
    value
  }

}
