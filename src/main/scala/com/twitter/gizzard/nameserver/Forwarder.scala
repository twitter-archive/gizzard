package com.twitter.gizzard.nameserver

import com.twitter.gizzard.shards._
import com.twitter.gizzard.scheduler._


class InvalidTableId(id: Int) extends NoSuchElementException("Table "+id+" not supported by Forwarder")

class Forwarder[T](
  nameServer: NameServer,
  shardRepo: ShardRepository,
  tableIdValidator: Int => Boolean,
  shardFactories: Map[String, ShardFactory[T]],
  copyFactory: CopyJobFactory[T],
  repairFactory: RepairJobFactory[T],
  diffFactory: RepairJobFactory[T]) // XXX: Why is this the same class???
extends ((Int, Long) => RoutingNode[T]) {

  // initialization
  for (pair <- shardFactories) shardRepo += pair
  val shardTypes = shardFactories.keySet.toSeq

  def isValidTableId(id: Int) = tableIdValidator(id)

  def containsShard(id: ShardId) = isValidShardInfo(nameServer.getShardInfo(id))

  def isValidShardInfo(info: ShardInfo) = shardTypes contains info.className

  def apply(tableId: Int, baseId: Long) = find(tableId, baseId)

  def find(tableId: Int, baseId: Long) = {
    if(isValidTableId(tableId)) {
      cast(nameServer.findCurrentForwarding(tableId, baseId))
    } else {
      throw new InvalidTableId(tableId)
    }
  }

  def findShardById(id: ShardId) = {
    cast(nameServer.findShardById(id))
  }

  // XXX: copy, repair and diff live here for now, but it's a bit
  // jank. clean up the admin job situation.
  def newCopyJob(from: ShardId, to: ShardId) = copyFactory(from, to)
  def newRepairJob(ids: Seq[ShardId])        = repairFactory(ids)
  def newDiffJob(ids: Seq[ShardId])          = diffFactory(ids)

  // helpers

  private def cast(r: RoutingNode[_]): RoutingNode[T] = {
    r.asInstanceOf[RoutingNode[T]]
  }
}

class ForwarderBuilder[T] private[nameserver] (ns: NameServer, repo: ShardRepository) {
  var tableValidator: Int => Boolean     = x => true
  private val shards                     = Map.newBuilder[String, ShardFactory[T]]
  var copyFactory: CopyJobFactory[T]     = new NullCopyJobFactory("Copies not supported!")
  var repairFactory: RepairJobFactory[T] = new NullRepairJobFactory("Shard repair not supported!")
  var diffFactory: RepairJobFactory[T]   = new NullRepairJobFactory("Shard diff not supported!")

  def validTables: Set[Int] = Set()
  def validTables_=(ids: Seq[Int]) { tableValidator = ids.toSet }

  def +=(item: (String, ShardFactory[T])) { shards += item }

  def build() = {
    new Forwarder[T](ns, repo, tableValidator, shards.result, copyFactory, repairFactory, diffFactory)
  }
}
