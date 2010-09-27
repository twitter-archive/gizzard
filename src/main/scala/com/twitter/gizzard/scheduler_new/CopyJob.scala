package com.twitter.gizzard.scheduler_new

import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger


object CopyJob {
  val MIN_COPY = 500
}

trait CopyFactory[S <: shards.Shard] extends ((shards.ShardId, shards.ShardId) => CopyJob[S])

trait CopyJobParser[S <: shards.Shard]
      extends JsonJobParser[(nameserver.NameServer[S], JobScheduler[CopyJob[S]]), CopyJob[S]] {
  def apply(codec: JsonCodec[(nameserver.NameServer[S], JobScheduler[CopyJob[S]]), CopyJob[S]], attributes: Map[String, Any]): CopyJob[S]
}


abstract case class CopyJob[S <: shards.Shard](sourceId: shards.ShardId, destinationId: shards.ShardId, var count: Int)
         extends JsonJob[(nameserver.NameServer[S], JobScheduler[CopyJob[S]])] {
  private val log = Logger.get(getClass.getName)

  def toMap = {
    Map("source_shard_hostname" -> sourceId.hostname,
        "source_shard_table_prefix" -> sourceId.tablePrefix,
        "destination_shard_hostname" -> destinationId.hostname,
        "destination_shard_table_prefix" -> destinationId.tablePrefix,
        "count" -> count
    ) ++ serialize
  }

  def finish(nameServer: nameserver.NameServer[S], scheduler: JobScheduler[CopyJob[S]]) {
    nameServer.markShardBusy(destinationId, shards.Busy.Normal)
    log.info("Copying finished for (type %s) from %s to %s",
             getClass.getName.split("\\.").last, sourceId, destinationId)
    Stats.clearGauge(gaugeName)
  }

  def apply(environment: (nameserver.NameServer[S], JobScheduler[CopyJob[S]])) {
    val (nameServer, scheduler) = environment
    try {
      log.info("Copying shard block (type %s) from %s to %s: state=%s",
               getClass.getName.split("\\.").last, sourceId, destinationId, toMap)
      val sourceShard = nameServer.findShardById(sourceId)
      val destinationShard = nameServer.findShardById(destinationId)
      // do this on each iteration, so it happens in the queue and can be retried if the db is busy:
      nameServer.markShardBusy(destinationId, shards.Busy.Busy)

      val nextJob = copyPage(sourceShard, destinationShard, count)
      nextJob match {
        case Some(job) => {
          incrGauge
          scheduler.put(job)
        }
        case None => finish(nameServer, scheduler)
      }
    } catch {
      case e: nameserver.NonExistentShard =>
        log.error("Shard block copy failed because one of the shards doesn't exist. Terminating the copy.")
      case e: shards.ShardTimeoutException if (count > CopyJob.MIN_COPY) =>
        log.warning("Shard block copy timed out; trying a smaller block size.")
        count = (count * 0.9).toInt
        scheduler.put(this)
      case e: shards.ShardDatabaseTimeoutException =>
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
