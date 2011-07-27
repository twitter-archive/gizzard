package com.twitter.gizzard.scheduler

import com.twitter.util.TimeConversions._
import com.twitter.logging.Logger
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
trait CopyJobFactory[T] extends ((ShardId, ShardId) => CopyJob[T])

class NullCopyJobFactory[T](message: String) extends CopyJobFactory[T] {
  def apply(from: ShardId, to: ShardId) = throw new UnsupportedOperation(message)
}

/**
 * A parser that creates a copy job out of json. The basic attributes (source shard ID, destination)
 * shard ID, and count) are parsed out first, and the remaining attributes are passed to
 * 'deserialize' to decode any shard-specific data (like a cursor).
 */
trait CopyJobParser[T] extends JsonJobParser {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId,
                  destinationId: ShardId, count: Int): CopyJob[T]

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
abstract case class CopyJob[T](sourceId: ShardId,
                               destinationId: ShardId,
                               var count: Int,
                               nameServer: NameServer,
                               scheduler: JobScheduler)
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
    nameServer.shardManager.markShardBusy(destinationId, Busy.Normal)
    log.info("Copying finished for (type %s) from %s to %s",
             getClass.getName.split("\\.").last, sourceId, destinationId)
    Stats.clearGauge(gaugeName)
  }

  def apply() {
    try {
      if (nameServer.shardManager.getShard(destinationId).busy == Busy.Cancelled) {
        log.info("Copying cancelled for (type %s) from %s to %s",
                 getClass.getName.split("\\.").last, sourceId, destinationId)
        Stats.clearGauge(gaugeName)

      } else {

        // XXX: get rid of the cast here by hooking off of the forwardingManager or equivalent
        val sourceShard      = nameServer.findShardById(sourceId).asInstanceOf[RoutingNode[T]]
        val destinationShard = nameServer.findShardById(destinationId).asInstanceOf[RoutingNode[T]]

        log.info("Copying shard block (type %s) from %s to %s: state=%s",
                 getClass.getName.split("\\.").last, sourceId, destinationId, toMap)
        // do this on each iteration, so it happens in the queue and can be retried if the db is busy:
        nameServer.shardManager.markShardBusy(destinationId, Busy.Busy)

        this.nextJob = copyPage(sourceShard, destinationShard, count)
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
          nameServer.shardManager.markShardBusy(destinationId, Busy.Error)
          throw e
        }
      case e: ShardDatabaseTimeoutException =>
        log.warning("Shard block copy failed to get a database connection; retrying.")
        scheduler.put(this)
      case e: Throwable =>
        log.error(e, "Shard block copy stopped due to exception: %s", e)
        nameServer.shardManager.markShardBusy(destinationId, Busy.Error)
        throw e
    }
  }

  private def incrGauge = {
    Stats.setGauge(gaugeName, Stats.getGauge(gaugeName).getOrElse(0.0) + count)
  }

  private def gaugeName = {
    "x-copying-" + sourceId + "-" + destinationId
  }

  def copyPage(sourceShard: RoutingNode[T], destinationShard: RoutingNode[T], count: Int): Option[CopyJob[T]]

  def serialize: Map[String, Any]
}
