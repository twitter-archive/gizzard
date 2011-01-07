package com.twitter.gizzard.scheduler.cursor

import scala.collection._
import com.twitter.gizzard.Jsonable
import com.twitter.gizzard.shards._


trait Destination extends Endpoint {
  val serializeName = "destination"
}

class DestinationImpl(var shardId: ShardId) extends Destination

object Destination extends EndpointUtils {
  val serializeName = "source"
  
  def apply(map: Map[String, Any]) = {
    new DestinationImpl(shardIdFromMap(map))
  }
  
  def apply(shardId: ShardId) = {
    new DestinationImpl(shardId)
  }
}
