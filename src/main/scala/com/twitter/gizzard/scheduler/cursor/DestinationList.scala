package com.twitter.gizzard.scheduler.cursor

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable._
import com.twitter.gizzard.Jsonable
import com.twitter.gizzard.shards._

object DestinationList {
  def apply(map: collection.Map[String, Any]): DestinationList = {
    val list = new DestinationList
    // FIXME
    list
  }
  
  def apply(shardId: ShardId): DestinationList = apply(Seq(shardId))
  def apply(shardIds: Collection[ShardId]): DestinationList = {
    val list = new DestinationList
    shardIds.foreach { id => list + Destination(id) }
    list
  }
}

class DestinationList extends ArrayBuffer[Destination] with Jsonable {
  def toMap: Map[String, Any] = {
    var map = Map.empty[String, Any]
    var i = 0
    this.foreach { dest => 
      map ++= dest.toMap(i)
      i += 1
    }
    map
  }
}
// 
// private def parseDestinations(attributes: Map[String, Any]) = {
//   val destinations = DestinationLi
//   var i = 0
//   while(attributes.contains("destination_" + i + "_hostname")) {
//     val prefix = "destination_" + i
//     val baseKey = prefix + "_base_id"
//     val baseId = if (attributes.contains(baseKey)) {
//       Some(attributes(baseKey).asInstanceOf[{def toLong: Long}].toLong)
//     } else {
//       None
//     }
//     val shardId = ShardId(attributes(prefix + "_shard_hostname").toString, attributes(prefix + "_shard_table_prefix").toString)
//     destinations += CopyDestination(shardId, baseId)
//     i += 1
//   }
// 
//   destinations.toList
// }
