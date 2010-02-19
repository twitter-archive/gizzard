package com.twitter.gizzard.jobs

import java.lang.reflect.Constructor
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import shards.{Busy, Shard, ShardTimeoutException}
import nameserver.NameServer


object CopyMachine {
  val MIN_COPY = 500

  def pack(sourceShardId: Int, destinationShardId: Int, count: Int): Map[String, AnyVal] = {
    Map("source_shard_id" -> sourceShardId, "destination_shard_id" -> destinationShardId, "count" -> count)
  }
}

abstract class CopyMachine[S <: Shard](attributes: Map[String, AnyVal]) extends UnboundJob[(NameServer[S], JobScheduler)] {
  val log = Logger.get(getClass.getName)
  val constructor = getClass.asInstanceOf[Class[CopyMachine[S]]].getConstructor(classOf[Map[String, AnyVal]])
  val (sourceShardId, destinationShardId, count) = (attributes("source_shard_id").toInt, attributes("destination_shard_id").toInt, attributes("count").toInt)

  var asMap: Map[String, AnyVal] = attributes
  def toMap = asMap

  def start(nameServer: NameServer[S], scheduler: JobScheduler) {
    scheduler(this)
  }

  def finish(nameServer: NameServer[S], scheduler: JobScheduler) {
    nameServer.markShardBusy(destinationShardId, Busy.Normal)
    log.info("Copying finished for (type %s) from %d to %d",
             getClass.getName.split("\\.").last, sourceShardId, destinationShardId)
  }

  def apply(environment: (NameServer[S], JobScheduler)) {
    val (nameServer, scheduler) = environment
    val newMap = try {
      log.info("Copying shard block (type %s) from %d to %d: state=%s",
               getClass.getName.split("\\.").last, sourceShardId, destinationShardId, attributes)
      val sourceShard = nameServer.findShardById(sourceShardId)
      val destinationShard = nameServer.findShardById(destinationShardId)
      // do this on each iteration, so it happens in the queue and can be retried if the db is busy:
      nameServer.markShardBusy(destinationShardId, Busy.Busy)
      copyPage(sourceShard, destinationShard, count)
    } catch {
      case e: NameServer.NonExistentShard =>
        log.error("Shard block copy failed because one of the shards doesn't exist. Terminating the copy.")
        return
      case e: ShardTimeoutException if (count > CopyMachine.MIN_COPY) =>
        log.warning("Shard block copy timed out; trying a smaller block size.")
        asMap = asMap ++ Map("count" -> count / 2)
        scheduler(constructor.newInstance(asMap))
        return
      case e: Exception =>
        log.warning("Shard block copy stopped due to exception: %s", e)
        throw e
    }
    asMap = newMap
    if (finished) {
      finish(nameServer, scheduler)
    } else {
      scheduler(constructor.newInstance(newMap))
    }
  }

  protected def copyPage(sourceShard: S, destinationShard: S, count: Int): Map[String, AnyVal]
  protected def finished: Boolean
}
