package com.twitter.gizzard.scheduler.cursor

import com.twitter.gizzard.shards.ShardId
import scala.collection.immutable._

trait EndpointUtils {
  val serializeName: String
  def shardIdFromMap(map: collection.Map[String, Any]): ShardId = shardIdFromMap(map, 0)
  def shardIdFromMap(map: collection.Map[String, Any], i: Int): ShardId = {
    ShardId(map(serializeName + "_" + i + "_hostname").toString, map(serializeName + "_" + i + "_tableprefix").toString)
  }
}

trait Endpoint[S <: shards.Shard] {
  val serializeName: String
  
  def toMap: Map[String, Any] = toMap(0)
  
  def shardId: ShardId 
  
  def shard: S 

  def toMap(i: Int): Map[String, Any] = {
    Map(
      serializeName + "_" + i + "_hostname" -> shardId.hostname,
      serializeName + "_" + i + "_table_prefix" -> shardId.tablePrefix
    )
  }
}