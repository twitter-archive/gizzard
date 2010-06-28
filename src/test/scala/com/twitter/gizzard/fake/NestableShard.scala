package com.twitter.gizzard.fake

import shards.ShardException
import org.specs.mock.{ClassMocker, JMocker}
import scala.collection.mutable

class NestableShardFactory[ConcreteShard <: Shard](readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard) extends shards.ShardFactory[ConcreteShard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[ConcreteShard]) =
    readWriteShardAdapter(new NestableShard(shardInfo, weight, children))
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class NestableShard(val shardInfo: ShardInfo, val weight:Int, val children: Seq[Shard]) extends Shard {
  val map = new mutable.HashMap[String, String]
  
  def get(key: String) = {
    map.get(key)
  }
  
  def put(key: String, value: String) = {
    map.put(key, value)
  }
  
}