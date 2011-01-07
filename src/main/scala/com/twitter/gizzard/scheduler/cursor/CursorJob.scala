package com.twitter.gizzard.scheduler.cursor

import net.lag.logging.Logger
import com.twitter.gizzard.shards._
import com.twitter.gizzard.nameserver.{NameServer, NonExistentShard, InvalidShard, NameserverUninitialized}

import com.twitter.ostrich.Stats
import scala.collection.mutable.Map

object CursorJob {
  val MIN_PAGE = 500
}

/**
 * A json-encodable job that represents the state of a copy-like operation from one shard to another.
 *
 * The 'toMap' implementation encodes the source and destination shard IDs, and the count of items.
 * Other shard-specific data (like the cursor) can be encoded in 'serialize'.
 *
 * 'cursorPage' is called to do the actual data cursoring. It should return a new CursorJob representing
 * the next chunk of work to do, or None if the entire job is complete.
 */
abstract case class CursorJob[S <: Shard](source: Source,
                                        destinations: DestinationList,
                                        var count: Int,
                                        nameServer: NameServer[S],
                                        scheduler: JobScheduler[JsonJob])
         extends JsonJob {
  private val log = Logger.get(getClass.getName)

  override def shouldReplicate = false
  
  def name: String

  def toMap = {
    source.toMap ++ serialize ++ destinations.toMap
  }

  def finish() {
    destinations.foreach { dest =>
      nameServer.markShardBusy(dest.shardId, shards.Busy.Normal)
    }
    log.info("Finished for (type %s) from %s to %s",
             getClass.getName.split("\\.").last, source, destinations)
    Stats.clearGauge(gaugeName)
  }

  def apply() {
    try {
      log.info("Shard block (type %s) from %s to %s: state=%s",
               getClass.getName.split("\\.").last, source, destinations, toMap)

      // do this on each iteration, so it happens in the queue and can be retried if the db is busy:
      destinations.foreach { dest =>
        nameServer.markShardBusy(dest.shardId, shards.Busy.Busy)
      }
      
      val nextJob = cursorPage(source, destinations, count)
      
      nextJob match {
        case Some(job) =>
          incrGauge
          scheduler.put(job)
        case None =>
          finish()
      }
    } catch {
      case e: NonExistentShard =>
        log.error("Shard block "+name+" failed because one of the shards doesn't exist. Terminating...")
      case e: ShardTimeoutException if (count > CursorJob.MIN_PAGE) =>
        log.warning("Shard block "+name+" timed out; trying a smaller block size.")
        count = (count * 0.9).toInt
        scheduler.put(this)
      case e: ShardDatabaseTimeoutException =>
        log.warning("Shard block "+name+" failed to get a database connection; retrying.")
        scheduler.put(this)
      case e: Throwable =>
        log.warning("Shard block "+name+" stopped due to exception: %s", e)
        throw e
    }
  }

  private def incrGauge = {
    Stats.setGauge(gaugeName, Stats.getGauge(gaugeName).getOrElse(0.0) + count)
  }

  private def gaugeName = {
    "x-"+name+"-" + source + "-" + destinations
  }

  def cursorPage(source: Source, destinations: DestinationList, count: Int): Option[CursorJob[S]]

  def serialize: collection.Map[String, Any] = Map.empty[String, Any]
}
