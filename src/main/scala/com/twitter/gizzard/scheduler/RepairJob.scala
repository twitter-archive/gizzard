package com.twitter.gizzard.scheduler

import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import net.lag.logging.Logger
import nameserver.{NameServer, NonExistentShard}
import collection.mutable.ListBuffer
import shards.{Shard, ShardId, ShardDatabaseTimeoutException, ShardTimeoutException}

trait Repairable[T] {
  def similar(other: T): Int
}

object RepairJob {
  val MIN_COPY = 500
}

/**
 * A factory for creating a new repair job (with default count and a starting cursor) from a source
 * and destination shard ID.
 */
trait RepairJobFactory[S <: Shard] extends (Seq[ShardId] => RepairJob[S])

/**
 * A parser that creates a repair job out of json. The basic attributes (source shard ID, destination)
 * shard ID, count) are parsed out first, and the remaining attributes are passed to
 * 'deserialize' to decode any shard-specific data (like a cursor).
 */
trait RepairJobParser[S <: Shard] extends JsonJobParser {
  def deserialize(attributes: Map[String, Any], shardIds: Seq[ShardId], count: Int): RepairJob[S]

  def apply(attributes: Map[String, Any]): JsonJob = {
    deserialize(attributes,
      attributes("shards").asInstanceOf[Seq[Map[String, Any]]].
        map((e: Map[String, Any]) => ShardId(e("hostname").toString, e("table_prefix").toString)),
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
abstract case class RepairJob[S <: Shard](shardIds: Seq[ShardId],
                                       var count: Int,
                                       nameServer: NameServer[S],
                                       scheduler: PrioritizingJobScheduler[JsonJob],
                                       priority: Int) extends JsonJob {
  private val log = Logger.get(getClass.getName)

  def finish() {
    log.info("Repair finished for (type %s) for %s",
             getClass.getName.split("\\.").last, shardIds.mkString(", "))
    Stats.clearGauge(gaugeName)
  }

  def apply() {
    try {
      log.info("Repairing shard block (type %s): state=%s",
               getClass.getName.split("\\.").last, toMap)
      val shards = shardIds.map(nameServer.findShardById(_))
      repair(shards)
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
    Map("shards" -> shardIds.map((id:ShardId) => Map("hostname" -> id.hostname, "table_prefix" -> id.tablePrefix)),
        "count" -> count
    ) ++ serialize
  }

  def incrGauge = {
    Stats.setGauge(gaugeName, Stats.getGauge(gaugeName).getOrElse(0.0) + 1)
  }

  private def gaugeName = {
    "x-repairing-" + shardIds.mkString("-")
  }

  def repair(shards: Seq[S])

  def serialize: Map[String, Any]
}

abstract class MultiShardRepair[S <: Shard, R <: Repairable[R], C <: Any](shardIds: Seq[ShardId], cursor: C, count: Int,
    nameServer: NameServer[S], scheduler: PrioritizingJobScheduler[JsonJob], priority: Int) extends RepairJob(shardIds, count, nameServer, scheduler, priority) {

  def scheduleNextRepair(lowestItem: Option[R]): Unit

  def schedule(tableId: Int, item: R)

  def cursorAtEnd(cursor: C): Boolean

  def smallestList(listCursors: Seq[(ListBuffer[R], C)]) = {
    listCursors.map(_._1).filter(!_.isEmpty).reduceLeft((list1, list2) => if (list1(0).similar(list2(0)) < 0) list1 else list2)
  }

  def repairListCursor(listCursors: Seq[(ListBuffer[R], C)], tableIds: Seq[Int]) = {
    if (tableIds.forall((id) => id == tableIds(0))) {
      while (listCursors.forall(lc => !lc._1.isEmpty || cursorAtEnd(lc._2)) && listCursors.exists(lc => !lc._1.isEmpty)) {
        val tableId = tableIds(0)
        val firstList = smallestList(listCursors)
        val firstItem = firstList.remove(0)
        var firstEnqueued = false
        val similarLists = listCursors.map(_._1).filter(!_.isEmpty).filter(_ != firstList).filter(_(0).similar(firstItem) == 0)
        if (similarLists.size != (listCursors.size - 1) ) {
          firstEnqueued = true
          schedule(tableId, firstItem)
        }
        for (list <- similarLists) {
          if (firstItem == list(0)) {
            list.remove(0)
          } else {
            if (!firstEnqueued) {
              firstEnqueued = true
              schedule(tableId, firstItem)
            }
            schedule(tableId, list.remove(0))
          }
        }
      }
      scheduleNextRepair(if (listCursors.filter(!_._1.isEmpty).size == 0) None else Some(smallestList(listCursors)(0)))
    } else {
      throw new RuntimeException("tableIds didn't match")
    }
  }
}
