package com.twitter.gizzard
package scheduler

import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import net.lag.logging.Logger
import nameserver.{NameServer, NonExistentShard}
import collection.mutable.ListBuffer
import shards.{Shard, ShardId, ShardDatabaseTimeoutException, ShardTimeoutException}

trait Entity[T] {
  def similar(other: T): Int
  def isSchedulable(other: T): Boolean
}

object CopyJob {
  val MIN_COPY = 500
}

/**
 * A factory for creating a new copy job (with default count and a starting cursor) from a source
 * and destination shard ID.
 */
trait CopyJobFactory[S <: Shard] extends (Seq[ShardId] => CopyJob[S])

/**
 * A parser that creates a copy job out of json. The basic attributes (source shard ID, destination)
 * shard ID, count) are parsed out first, and the remaining attributes are passed to
 * 'deserialize' to decode any shard-specific data (like a cursor).
 */
trait CopyJobParser[S <: Shard] extends JsonJobParser {
  def deserialize(attributes: Map[String, Any], shardIds: Seq[ShardId], count: Int): CopyJob[S]

  def apply(attributes: Map[String, Any]): JsonJob = {
    deserialize(attributes,
      attributes("shards").asInstanceOf[Seq[Map[String, Any]]].
        map((e: Map[String, Any]) => ShardId(e("hostname").toString, e("table_prefix").toString)),
      attributes("count").asInstanceOf[{def toInt: Int}].toInt)
  }
}

/**
 * A json-encodable job that represents the state of a copy one a shard.
 *
 * The 'toMap' implementation encodes the source and destination shard IDs, and the count of items.
 * Other shard-specific data (like the cursor) can be encoded in 'serialize'.
 *
 * 'copy' is called to do the actual data copy. It should return a new Some[CopyJob] representing
 * the next chunk of work to do, or None if the entire copying job is complete.
 */
abstract case class CopyJob[S <: Shard](shardIds: Seq[ShardId],
                                       var count: Int,
                                       nameServer: NameServer[S],
                                       scheduler: PrioritizingJobScheduler,
                                       priority: Int) extends JsonJob {
  private val log = Logger.get(getClass.getName)

  override def shouldReplicate = false

  def finish() {
    log.info("[Copy] - finished for (type %s) for %s",
             getClass.getName.split("\\.").last, shardIds.mkString(", "))
    Stats.clearGauge(gaugeName)
  }

  def apply() {
    try {
      log.info("[Copy] - shard block (type %s): state=%s",
               getClass.getName.split("\\.").last, toMap)
      val shardObjs = shardIds.map(nameServer.findShardById(_))
      shardIds.foreach(nameServer.markShardBusy(_, shards.Busy.Busy))
      copy(shardObjs)
      this.nextJob match {
        case None => shardIds.foreach(nameServer.markShardBusy(_, shards.Busy.Normal))
        case _ =>
      }
    } catch {
      case e: NonExistentShard =>
        log.error("[Copy] - failed because one of the shards doesn't exist. Terminating the copy.")
      case e: ShardDatabaseTimeoutException =>
        log.warning("[Copy] - failed to get a database connection; retrying.")
        scheduler.put(priority, this)
      case e: ShardTimeoutException if (count > CopyJob.MIN_COPY) =>
        log.warning("[Copy] - block copy timed out; trying a smaller block size.")
        count = (count * 0.9).toInt
        scheduler.put(priority, this)
      case e: Throwable =>
        log.warning(e, "[Copy] - stopped due to exception: %s", e)
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
    "x-copy-" + shardIds.mkString("-")
  }

  def copy(shards: Seq[S])

  def serialize: Map[String, Any]
}

abstract class MultiShardCopy[S <: Shard, R <: Entity[R], C <: Ordered[C]](shardIds: Seq[ShardId], cursor: C, count: Int,
    nameServer: NameServer[S], scheduler: PrioritizingJobScheduler, priority: Int) extends CopyJob(shardIds, count, nameServer, scheduler, priority) {

  private val log = Logger.get(getClass.getName)

  def cursorAtEnd(cursor :C): Boolean

  def nextCopy(lowestCursor: C): Option[CopyJob[S]]

  def scheduleItem(missing: Boolean, list: (S, ListBuffer[R], C), tableId: Int, item: R): Unit

  def scheduleBulk(otherShards: Seq[S], items: Seq[R]): Unit

  def smallestList(listCursors: Seq[(S, ListBuffer[R], C)]) = {
    listCursors.filter(!_._2.isEmpty).reduceLeft((list1, list2) => if (list1._2(0).similar(list2._2(0)) < 0) list1 else list2)
  }

  def select(shard: S, cursor: C, count: Int): (Seq[R], C)

  def copy(shards: Seq[S]) = {
    val tableIds = shards.map(shard => nameServer.getRootForwardings(shard.shardInfo.id).head.tableId)

    val listCursors = shards.map( (shard) => {
      val (seq, newCursor) = select(shard, cursor, count)
      val list = new ListBuffer[R]()
      list ++= seq
      (shard, list, newCursor)
    })
    copyListCursor(listCursors, tableIds)
  }

  private def copyListCursor(listCursors: Seq[(S, ListBuffer[R], C)], tableIds: Seq[Int]) = {
    if (!tableIds.forall((id) => id == tableIds(0))) {
      throw new RuntimeException("tableIds didn't match")
    } else if (nameServer.getCommonShardId(shardIds) == None) {
      throw new RuntimeException("these shardIds don't have a common ancestor")
    } else {
      while (listCursors.forall(lc => !lc._2.isEmpty || cursorAtEnd(lc._3)) && listCursors.exists(lc => !lc._2.isEmpty)) {
        val tableId = tableIds(0)
        val firstList = smallestList(listCursors)
        val finishedLists = listCursors.filter(lc => cursorAtEnd(lc._3) && lc._2.isEmpty)
        if (finishedLists.size == listCursors.size - 1) {
          scheduleBulk(finishedLists.map(_._1), firstList._2)
          firstList._2.clear
        } else {
          val firstItem = firstList._2.remove(0)
          var firstEnqueued = false
          val similarLists = listCursors.filter(!_._2.isEmpty).filter(_._1 != firstList._1).filter(_._2(0).similar(firstItem) == 0)
          if (similarLists.size != (listCursors.size - 1) ) {
            firstEnqueued = true
            scheduleItem(true, firstList, tableId, firstItem)
          }
          for (list <- similarLists) {
            val similarItem = list._2.remove(0)
            if (firstItem.isSchedulable(similarItem)) {
              if (!firstEnqueued) {
                firstEnqueued = true
                scheduleItem(false, firstList, tableId, firstItem)
              }
              scheduleItem(false, list, tableId, similarItem)
            }
          }
        }
      }
      val nextCursor = listCursors.map(_._3).reduceLeft((c1, c2) => if (c1.compare(c2) <= 0) c1 else c2)
      this.nextJob = nextCopy(nextCursor)
    }
  }
}
