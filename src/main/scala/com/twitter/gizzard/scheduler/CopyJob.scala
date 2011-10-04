package com.twitter.gizzard.scheduler

import com.twitter.util.TimeConversions._
import com.twitter.logging.Logger
import com.twitter.gizzard.Stats
import com.twitter.gizzard.nameserver.{NameServer, NonExistentShard}
import com.twitter.gizzard.shards._


object CopyJob {
  val MIN_COPY = 500
}

class UnsupportedOperation(msg: String) extends ShardException(msg)

/**
 * A factory for creating a new copy job (with default count and a starting cursor) from a source
 * and destination shard ID.
 */
trait CopyJobFactory[T] extends (Seq[ShardId] => CopyJob[T])

class NullCopyJobFactory[T](message: String) extends CopyJobFactory[T] {
  def apply(ids: Seq[ShardId]) = throw new UnsupportedOperation(message)
}

/**
 * A parser that creates a copy job out of json. The basic attributes (source shard ID, destination)
 * shard ID, and count) are parsed out first, and the remaining attributes are passed to
 * 'deserialize' to decode any shard-specific data (like a cursor).
 */
trait CopyJobParser[T] extends JsonJobParser {
  def deserialize(attributes: Map[String, Any], shardIds: Seq[ShardId], count: Int): CopyJob[T]

  def apply(attributes: Map[String, Any]): JsonJob = {
    deserialize(attributes,
                attributes("shards").asInstanceOf[Seq[Map[String, Any]]].map {idmap => ShardId(idmap("hostname").toString, idmap("table_prefix").toString)},
                attributes("count").asInstanceOf[{def toInt: Int}].toInt)
  }
}

/**
 * A json-encodable job that represents the state of a copy from one shard to another.
 *
 * The 'toMap' implementation encodes the source and destination shard IDs, and the count of items.
 * Other shard-specific data (like the cursor) can be encoded in 'serialize'.
 *
 * 'copyPage' is called to do the actual data copying. It should return a new CopyJob representing
 * the next chunk of work to do, or None if the entire copying job is complete.
 */
abstract case class CopyJob[T](shardIds: Seq[ShardId],
                               var count: Int,
                               nameServer: NameServer,
                               scheduler: JobScheduler)
         extends JsonJob {
  private val log = Logger.get(getClass.getName)
  override def shouldReplicate = false


  def toMap = {
    Map("shards" -> shardIds.map { id => Map(
                        "hostname" -> id.hostname, 
                        "table_prefix" -> id.tablePrefix
                    )}, 
        "count" -> count) ++ serialize
  }

  def finish() {
    shardIds.foreach { nameServer.shardManager.markShardBusy(_, Busy.Normal) }
    log.info("Copying finished for (type %s) between %s",
             getClass.getName.split("\\.").last, shardIds.mkString(", "))
    Stats.clearGauge(gaugeName)
  }

  def apply() {
    try {
      if (shardIds.map { nameServer.shardManager.getShard(_).busy }.find(_ == Busy.Cancelled).isDefined) {
        log.info("Copying cancelled for (type %s) between %s",
                 getClass.getName.split("\\.").last, shardIds.mkString(", "))
        Stats.clearGauge(gaugeName)

      } else {

        // XXX: get rid of the cast here by hooking off of the forwardingManager or equivalent
        val shards = shardIds.map { nameServer.findShardById(_).asInstanceOf[RoutingNode[T]] }

        log.info("Copying shard block (type %s) between %s: state=%s",
                 getClass.getName.split("\\.").last, shardIds.mkString(", "), toMap)
        // do this on each iteration, so it happens in the queue and can be retried if the db is busy:
        shardIds.foreach { nameServer.shardManager.markShardBusy(_, Busy.Busy) }

        this.nextJob = copyPage(shards, count)
        this.nextJob match {
          case None => finish()
          case _ => incrGauge
        }
      }
    } catch {
      case e: NonExistentShard =>
        log.error("Shard block copy failed because one of the shards doesn't exist. Terminating the copy.")
      case e: ShardTimeoutException =>
        if (count > CopyJob.MIN_COPY) {
          log.warning("Shard block copy timed out; trying a smaller block size.")
          count = (count * 0.9).toInt
          scheduler.put(this)
        } else {
          log.error("Shard block copy timed out on minimum block size.")
          shardIds.foreach { nameServer.shardManager.markShardBusy(_, Busy.Error) }
          throw e
        }
      case e: ShardDatabaseTimeoutException =>
        log.warning("Shard block copy failed to get a database connection; retrying.")
        scheduler.put(this)
      case e: Throwable =>
        log.error(e, "Shard block copy stopped due to exception: %s", e)
        shardIds.foreach { nameServer.shardManager.markShardBusy(_, Busy.Error) }
        throw e
    }
  }

  private def incrGauge = {
    Stats.setGauge(gaugeName, Stats.getGauge(gaugeName).getOrElse(0.0) + count)
  }

  private def gaugeName = {
      "x-copying-" + shardIds.mkString("-")
  }

  def copyPage(nodes: Seq[RoutingNode[T]], count: Int): Option[CopyJob[T]]

  def serialize: Map[String, Any]
}
