package com.twitter.gizzard.scheduler

import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import net.lag.logging.Logger
import nameserver.{NameServer, NonExistentShard}
import collection.mutable.ListBuffer
import shards.{Shard, ShardId, ShardDatabaseTimeoutException, ShardTimeoutException}

object RepairJob {
  val MIN_COPY = 500
}

trait Repairable[T] {
  def similar(other: T): Int
}

/**
 * A factory for creating a new repair job (with default count and a starting cursor) from a source
 * and destination shard ID.
 */
trait RepairJobFactory[S <: Shard, R <: Repairable[R]] extends ((ShardId, ShardId) => RepairJob[S, R])

/**
 * A parser that creates a repair job out of json. The basic attributes (source shard ID, destination)
 * shard ID, count) are parsed out first, and the remaining attributes are passed to
 * 'deserialize' to decode any shard-specific data (like a cursor).
 */
trait RepairJobParser[S <: Shard, R <: Repairable[R]] extends JsonJobParser {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int): RepairJob[S, R]

  def apply(attributes: Map[String, Any]): JsonJob = {
    deserialize(attributes,
      ShardId(attributes("source_shard_hostname").toString, attributes("source_shard_table_prefix").toString),
      ShardId(attributes("destination_shard_hostname").toString, attributes("destination_shard_table_prefix").toString),
      attributes("count").asInstanceOf[{def toInt: Int}].toInt)
  }
}

/**
 * A json-encodable job that represents the state of a repair one a shard.
 *
 * The 'toMap' implementation encodes the source and destination shard IDs, and the count of items.
 * Other shard-specific data (like the cursor) can be encoded in 'serialize'.
 *
 * 'repair' is called to do the actual data repair. It should return a new Some[RepairJob] representing
 * the next chunk of work to do, or None if the entire copying job is complete.
 */
abstract case class RepairJob[S <: Shard, R <: Repairable[R]](sourceId: ShardId,
                                       destinationId: ShardId,
                                       var count: Int,
                                       nameServer: NameServer[S],
                                       scheduler: PrioritizingJobScheduler[JsonJob],
                                       priority: Int) extends JsonJob {
  private val log = Logger.get(getClass.getName)

  def finish() {
    log.info("Repair finished for (type %s) from %s to %s",
             getClass.getName.split("\\.").last, sourceId, destinationId)
    Stats.clearGauge(gaugeName)
  }

  def apply() {
    try {
      log.info("Repairing shard block (type %s) from %s to %s: state=%s",
               getClass.getName.split("\\.").last, sourceId, destinationId, toMap)
      val sourceShard = nameServer.findShardById(sourceId)
      val destinationShard = nameServer.findShardById(destinationId)
      repair(sourceShard, destinationShard)
    } catch {
      case e: NonExistentShard =>
        log.error("Shard block repair failed because one of the shards doesn't exist. Terminating the repair.")
      case e: ShardDatabaseTimeoutException =>
        log.warning("Shard block repair failed to get a database connection; retrying.")
        scheduler.put(priority, this)
      case e: ShardTimeoutException if (count > RepairJob.MIN_COPY) =>
        log.warning("Shard block copy timed out; trying a smaller block size.")
        count = (count * 0.9).toInt
        scheduler.put(priority, this)
      case e: Throwable =>
        log.warning("Shard block repair stopped due to exception: %s", e)
        throw e
    }
  }

  def toMap = {
    Map("source_shard_hostname" -> sourceId.hostname,
        "source_shard_table_prefix" -> sourceId.tablePrefix,
        "destination_shard_hostname" -> destinationId.hostname,
        "destination_shard_table_prefix" -> destinationId.tablePrefix,
        "count" -> count
    ) ++ serialize
  }

  def incrGauge = {
    Stats.setGauge(gaugeName, Stats.getGauge(gaugeName).getOrElse(0.0) + 1)
  }

  private def gaugeName = {
    "x-repairing-" + sourceId + "-" + destinationId
  }

  def repair(sourceShard: S, destinationShard: S)

  def serialize: Map[String, Any]
  
  def enqueueFirst(tableId: Int, list: ListBuffer[R])
  
  def resolve(tableId: Int, srcSeq: Seq[R], srcCursorAtEnd: Boolean, destSeq: Seq[R], destCursorAtEnd: Boolean) = {
    val srcItems = new ListBuffer[R]()
    srcItems ++= srcSeq
    val destItems = new ListBuffer[R]()
    destItems ++= destSeq
    var running = !(srcItems.isEmpty && destItems.isEmpty)
    while (running) {
      val srcItem = srcItems.firstOption
      val destItem = destItems.firstOption
      (srcCursorAtEnd, destCursorAtEnd, srcItem, destItem) match {
        case (true, true, None, None) => running = false
        case (true, true, _, None) => enqueueFirst(tableId, srcItems)
        case (true, true, None, _) => enqueueFirst(tableId, destItems)
        case (_, _, _, _) =>
          (srcItem, destItem) match {
            case (None, None) => running = false
            case (_, None) => running = false
            case (None, _) => running = false
            case (_, _) =>
              srcItem.get.similar(destItem.get) match {
                case x if x < 0 => enqueueFirst(tableId, srcItems)
                case x if x > 0 => enqueueFirst(tableId, destItems)
                case _ =>
                  if (srcItem != destItem) {
                    enqueueFirst(tableId, srcItems)
                    enqueueFirst(tableId, destItems)
                  } else {
                    srcItems.remove(0)
                    destItems.remove(0)
                  }
              }
          }
      }
      running &&= !(srcItems.isEmpty && destItems.isEmpty)
    }
    (srcItems.firstOption, destItems.firstOption)
  }
}
