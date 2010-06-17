package com.twitter.gizzard.fake
import shards.{ShardInfo, ShardId, Busy}
import scala.collection.mutable

class ConcreteShard extends Shard {
  val weight = 1;
  val children = List()
  private val rand = new java.util.Random()
  private val uuid = java.util.UUID.randomUUID.toString.replaceAll("-", "")
  val shardInfo = ShardInfo(ShardId("something", uuid), null, null, null, Busy.Normal)
  val shardId = shardInfo.id
  private val map = mutable.Map.empty[String, String]
  
  def get(key: String) = map.get(key)
  def put(key: String, value: String) = map.put(key, value)
}