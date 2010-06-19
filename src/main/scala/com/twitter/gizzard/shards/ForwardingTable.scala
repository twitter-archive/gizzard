package com.twitter.gizzard.shards

import scala.collection.mutable
import java.util.TreeMap

class AddressOutOfBounds extends Exception

class ForwardingTable(val forwardings: Seq[Forwarding]) {
  private var data = new mutable.HashMap[Int, TreeMap[Long, Shard]]
  
  forwardings.foreach { forwarding => 
    val address = forwarding.address
    val treeMap = data.getOrElseUpdate(address.tableId, new TreeMap[Long, Shard])
    treeMap.put(address.baseId, forwarding.shard)
  }
  
  def forwardingsForShard(shard: Shard) = {
    forwardings.filter(_.shard.equalsOrContains(shard))
  }
    
  def getShard(address: Address) = {
    data.get(address.tableId).flatMap { byBaseIds =>
      val item = byBaseIds.floorEntry(address.baseId)
      if (item == null) {
        None
      } else {
        Some(item.getValue)
      }
    } getOrElse {
      throw new AddressOutOfBounds
    }
  }
}
