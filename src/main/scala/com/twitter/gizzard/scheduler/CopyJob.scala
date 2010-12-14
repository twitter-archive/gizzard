package com.twitter.gizzard.scheduler

import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import net.lag.logging.Logger
import nameserver.{NameServer, NonExistentShard}
import shards.{Shard, CopyableShard, CopyPage, ShardId, ShardDatabaseTimeoutException, ShardTimeoutException}

object CopyJob {
  val MIN_COPY = 500
}

/**
 * A factory for creating a new copy job (with default count and a starting cursor) from a source
 * and destination shard ID.
 */
trait CopyJobFactory[S <: Shard] extends ((ShardId, ShardId) => CopyJob[S])

/**
 * A parser that creates a copy job out of json. The basic attributes (source shard ID, destination)
 * shard ID, and count) are parsed out first, and the remaining attributes are passed to
 * 'deserialize' to decode any shard-specific data (like a cursor).
 */
trait CopyJobParser[S <: Shard] extends JsonJobParser {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId,
                  destinationId: ShardId, count: Int): CopyJob[S]

  def apply(attributes: Map[String, Any]): JsonJob = {
    deserialize(attributes,
                ShardId(attributes("source_shard_hostname").toString, attributes("source_shard_table_prefix").toString),
                ShardId(attributes("destination_shard_hostname").toString, attributes("destination_shard_table_prefix").toString),
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
abstract case class CopyJob[S <: Shard](
  sourceId: ShardId,
  destinationId: ShardId,
  var count: Int,
  nameServer: NameServer[S],
  scheduler: JobScheduler[JsonJob])
extends JsonJob {
  private val log = Logger.get(getClass.getName)

  override def shouldReplicate = false

  def toMap = {
    Map("source_shard_hostname" -> sourceId.hostname,
        "source_shard_table_prefix" -> sourceId.tablePrefix,
        "destination_shard_hostname" -> destinationId.hostname,
        "destination_shard_table_prefix" -> destinationId.tablePrefix,
        "count" -> count
    ) ++ serialize
  }

  def finish() {
    nameServer.markShardBusy(destinationId, shards.Busy.Normal)
    log.info("Copying finished for (type %s) from %s to %s",
             getClass.getName.split("\\.").last, sourceId, destinationId)
    Stats.clearGauge(gaugeName)
  }

  def apply() {
    try {
      log.info("Copying shard block (type %s) from %s to %s: state=%s",
               getClass.getName.split("\\.").last, sourceId, destinationId, toMap)
      val sourceShard = nameServer.findShardById(sourceId)
      val destinationShard = nameServer.findShardById(destinationId)
      // do this on each iteration, so it happens in the queue and can be retried if the db is busy:
      nameServer.markShardBusy(destinationId, shards.Busy.Busy)

      val nextJob = copyPage(sourceShard, destinationShard, count)
      nextJob match {
        case Some(job) =>
          incrGauge
          scheduler.put(job)
        case None =>
          finish()
      }
    } catch {
      case e: NonExistentShard =>
        log.error("Shard block copy failed because one of the shards doesn't exist. Terminating the copy.")
      case e: ShardTimeoutException if (count > CopyJob.MIN_COPY) =>
        log.warning("Shard block copy timed out; trying a smaller block size.")
        count = (count * 0.9).toInt
        scheduler.put(this)
      case e: ShardDatabaseTimeoutException =>
        log.warning("Shard block copy failed to get a database connection; retrying.")
        scheduler.put(this)
      case e: Throwable =>
        log.warning("Shard block copy stopped due to exception: %s", e)
        throw e
    }
  }

  private def incrGauge = {
    Stats.setGauge(gaugeName, Stats.getGauge(gaugeName).getOrElse(0.0) + count)
  }

  private def gaugeName = {
    "x-copying-" + sourceId + "-" + destinationId
  }

  def copyPage(sourceShard: S, destinationShard: S, count: Int): Option[CopyJob[S]]

  def serialize: Map[String, Any]
}


class BasicCopyJobFactory[P <: CopyPage, S <: CopyableShard[P]](
  ns: NameServer[S],
  s: JobScheduler[JsonJob],
  count: Int)
extends CopyJobFactory[S] {
  def apply(sourceId: ShardId, destId: ShardId) = {
    new BasicCopyJob[P,S](sourceId, destId, None, count, ns, s)
  }
}

class BasicCopyJobParser[P <: CopyPage, S <: CopyableShard[P]](
  ns: NameServer[S],
  s: JobScheduler[JsonJob])
extends CopyJobParser[S] {
  def deserialize(attrs: Map[String, Any], sourceId: ShardId, destId: ShardId, count: Int) = {
    val cursor = attrs("cursor").asInstanceOf[Map[String,Any]]
    new BasicCopyJob[P,S](sourceId, destId, Some(cursor), count, ns, s)
  }
}

class BasicCopyJob[P <: CopyPage, S <: CopyableShard[P]](
  sourceId: ShardId,
  destId: ShardId,
  cursor: Option[Map[String, Any]],
  count: Int,
  nameServer: NameServer[S],
  scheduler: JobScheduler[JsonJob])
extends CopyJob[S](sourceId, destId, count, nameServer, scheduler) {

  def serialize = Map("cursor" -> cursor)

  def copyPage(source: S, dest: S, count: Int) = {
    val nextCursor = source.copyPage(dest, cursor, count)
    nextCursor.map(c => new BasicCopyJob[P,S](sourceId, destId, Some(c), count, nameServer, scheduler))
  }
}
