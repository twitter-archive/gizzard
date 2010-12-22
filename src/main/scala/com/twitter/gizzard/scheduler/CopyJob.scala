package com.twitter.gizzard.scheduler

import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import net.lag.logging.Logger
import collection.mutable.ListBuffer
import collection.mutable
import nameserver.{NameServer, NonExistentShard}
import shards._

object CopyJob {
  val MIN_COPY = 500
}

/**
 * A factory for creating a new copy job (with default count and a starting cursor) from a source
 * and destination shard ID.
 */
trait CopyJobFactory[S <: Shard] extends ((ShardId, List[CopyDestination]) => CopyJob[S])

case class CopyDestination(shardId: ShardId, baseId: Option[Long])
case class CopyDestinationShard[S](shard: S, baseId: Option[Long])

/**
 * A parser that creates a copy job out of json. The basic attributes (source shard ID, destination)
 * shard ID, and count) are parsed out first, and the remaining attributes are passed to
 * 'deserialize' to decode any shard-specific data (like a cursor).
 */
trait CopyJobParser[S <: Shard] extends JsonJobParser {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId,
                  destinations: List[CopyDestination], count: Int): CopyJob[S]

  def apply(attributes: Map[String, Any]): JsonJob = {
    val sourceId = ShardId(attributes("source_shard_hostname").toString, attributes("source_shard_table_prefix").toString)

    deserialize(attributes,
                sourceId,
                parseDestinations(attributes).toList,
                attributes("count").asInstanceOf[{def toInt: Int}].toInt)
  }

  private def parseDestinations(attributes: Map[String, Any]) = {
    val destinations = new ListBuffer[CopyDestination]
    var i = 0
    while(attributes.contains("destination_" + i + "_hostname")) {
      val prefix = "destination_" + i
      val baseKey = prefix + "_base_id"
      val baseId = if (attributes.contains(baseKey)) {
        Some(attributes(baseKey).asInstanceOf[{def toLong: Long}].toLong)
      } else {
        None
      }
      val shardId = ShardId(attributes(prefix + "_shard_hostname").toString, attributes(prefix + "_shard_table_prefix").toString)
      destinations += CopyDestination(shardId, baseId)
      i += 1
    }

    destinations.toList
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
abstract case class CopyJob[S <: Shard](sourceId: ShardId,
                                        destinations: List[CopyDestination],
                                        var count: Int,
                                        nameServer: NameServer[S],
                                        scheduler: JobScheduler[JsonJob])
         extends JsonJob {
  private val log = Logger.get(getClass.getName)

  override def shouldReplicate = false

  def toMap = {
    Map("source_shard_hostname" -> sourceId.hostname,
        "source_shard_table_prefix" -> sourceId.tablePrefix,
        "count" -> count
    ) ++ serialize ++ destinationsToMap
  }

  private def destinationsToMap = {
    var i = 0
    val map = mutable.Map[String, Any]()
    destinations.foreach { destination =>
      map("destination_" + i + "_hostname") = destination.shardId.hostname
      map("destination_" + i + "_table_prefix") = destination.shardId.tablePrefix
      destination.baseId.foreach { id =>
        map("destination_" + i + "_base_id") = id
      }
      i += 1
    }
    map
  }

  def finish() {
    destinations.foreach { dest =>
      nameServer.markShardBusy(dest.shardId, shards.Busy.Normal)
    }
    log.info("Copying finished for (type %s) from %s to %s",
             getClass.getName.split("\\.").last, sourceId, destinations)
    Stats.clearGauge(gaugeName)
  }

  def apply() {
    try {
      log.info("Copying shard block (type %s) from %s to %s: state=%s",
               getClass.getName.split("\\.").last, sourceId, destinations, toMap)
      val sourceShard = nameServer.findShardById(sourceId)

      val destinationShards = destinations.map { dest =>
        CopyDestinationShard[S](nameServer.findShardById(dest.shardId), dest.baseId)
      }

      // do this on each iteration, so it happens in the queue and can be retried if the db is busy:
      destinations.foreach { dest =>
        nameServer.markShardBusy(dest.shardId, shards.Busy.Busy)
      }
      
      val nextJob = copyPage(sourceShard, destinationShards, count)
      
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
    "x-copying-" + sourceId + "-" + destinations
  }

  def copyPage(sourceShard: S, destinationShards: List[CopyDestinationShard[S]], count: Int): Option[CopyJob[S]]

  def serialize: Map[String, Any]
}


class BasicCopyJobFactory[S <: Shard](
  ns: NameServer[S],
  s: JobScheduler[JsonJob],
  copyAdapter: ShardCopyAdapter[S],
  defaultCount: Int)
extends CopyJobFactory[S] {

  def apply(sourceId: ShardId, destId: ShardId) = {
    new BasicCopyJob(sourceId, destId, None, defaultCount, ns, s, copyAdapter)
  }

  def parser = new CopyJobParser[S] {
    def deserialize(attrs: Map[String, Any], sourceId: ShardId, destId: ShardId, count: Int) = {
      val cursor = attrs("cursor").asInstanceOf[Map[String,Any]]
      new BasicCopyJob(sourceId, destId, Some(cursor), count, ns, s, copyAdapter)
    }
  }
}

class BasicCopyJob[S <: Shard](
  sourceId: ShardId,
  destId: ShardId,
  cursor: Option[Map[String, Any]],
  count: Int,
  nameServer: NameServer[S],
  scheduler: JobScheduler[JsonJob],
  copyAdapter: ShardCopyAdapter[S])
extends CopyJob[S](sourceId, destId, count, nameServer, scheduler) {

  def serialize = Map("cursor" -> cursor)

  def copyPage(source: S, dest: S, count: Int) = {
    copyAdapter.copyPage(source, dest, cursor, count).map { nextCursor =>
      new BasicCopyJob(sourceId, destId, Some(nextCursor), count, nameServer, scheduler, copyAdapter)
    }
  }
}
