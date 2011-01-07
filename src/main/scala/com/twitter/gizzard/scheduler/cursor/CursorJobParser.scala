package com.twitter.gizzard.scheduler.cursor

import com.twitter.gizzard.shards._
import scala.collection._

trait CursorJobParser[S <: Shard] extends JsonJobParser {
  def deserialize(attributes: Map[String, Any], source: Source,
                  destinations: DestinationList, count: Int): CursorJob[S]

  def apply(attributes: Map[String, Any]): JsonJob = {
    deserialize(attributes,
                Source(attributes),
                DestinationList(attributes),
                attributes("count").asInstanceOf[{def toInt: Int}].toInt)
  }

}
