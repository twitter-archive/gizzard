package com.twitter.gizzard.scheduler.cursor

import scala.collection.immutable._
import com.twitter.gizzard.Jsonable
import com.twitter.gizzard.shards._


trait Source extends Endpoint {
  val serializeName = "source"
}

object Source extends EndpointUtils {
  val serializeName = "source"
  def apply(map: collection.Map[String, Any]) = {
    new SourceImpl(shardIdFromMap(map))
  }
  
  def apply(shardId: ShardId) = {
    new SourceImpl(shardId)
  }
    
}

class SourceImpl(var shardId: ShardId) extends Source
