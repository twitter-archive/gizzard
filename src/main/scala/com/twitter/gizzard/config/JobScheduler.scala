package com.twitter.gizzard.config

import com.twitter.util.{Duration, StorageUnit}
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import net.lag.logging.Logger
import net.lag.kestrel.PersistentQueue
import net.lag.kestrel.config.QueueConfig

import com.twitter.gizzard
import com.twitter.gizzard.scheduler
import com.twitter.gizzard.scheduler.{JsonJob, JsonCodec, MemoryJobQueue, KestrelJobQueue, JobConsumer}

trait SchedulerType
trait KestrelScheduler extends SchedulerType {
  var path = "/tmp"

  // redo config here
  var maxItems: Int                 = Int.MaxValue
  var maxSize: StorageUnit          = Long.MaxValue.bytes
  var maxItemSize: StorageUnit      = Long.MaxValue.bytes
  var maxAge: Option[Duration]      = None
  var maxJournalSize: StorageUnit   = 16.megabytes
  var maxMemorySize: StorageUnit    = 128.megabytes
  var maxJournalOverflow: Int       = 10
  var discardOldWhenFull: Boolean   = false
  var keepJournal: Boolean          = true
  var syncJournal: Boolean          = false
  var multifileJournal: Boolean     = false
  var expireToQueue: Option[String] = None
  var maxExpireSweep: Int           = Int.MaxValue
  var fanoutOnly: Boolean           = false

  def aConfig = QueueConfig(
    maxItems           = maxItems,
    maxSize            = maxSize,
    maxItemSize        = maxItemSize,
    maxAge             = maxAge,
    maxJournalSize     = maxJournalSize,
    maxMemorySize      = maxMemorySize,
    maxJournalOverflow = maxJournalOverflow,
    discardOldWhenFull = discardOldWhenFull,
    keepJournal        = keepJournal,
    syncJournal        = syncJournal,
    multifileJournal   = multifileJournal,
    expireToQueue      = expireToQueue,
    maxExpireSweep     = maxExpireSweep,
    fanoutOnly         = fanoutOnly
  )

  def apply(name: String): PersistentQueue = {
    new PersistentQueue(name, path, aConfig)
  }
}
class MemoryScheduler extends SchedulerType {
  var sizeLimit = 0
}

trait BadJobConsumer {
  def apply(): JobConsumer
}

class JsonJobLogger extends BadJobConsumer {
  var name = "bad_jobs"

  def apply(): JobConsumer = new scheduler.JsonJobLogger(Logger.get(name))
}

trait Scheduler {
  def name: String
  def schedulerType: SchedulerType
  var threads = 1
  var errorLimit          = 100
  var errorStrobeInterval = 30.seconds
  var errorRetryDelay     = 900.seconds
  var perFlushItemLimit   = 1000
  var jitterRate          = 0.0f

  var _jobQueueName: Option[String] = None
  def jobQueueName_=(s: String) { _jobQueueName = Some(s) }
  def jobQueueName: String = _jobQueueName getOrElse name
  var _errorQueueName: Option[String] = None
  def errorQueueName_=(s: String) { _errorQueueName = Some(s) }
  def errorQueueName: String = _errorQueueName.getOrElse(name + "_errors")
  var badJobQueue: Option[BadJobConsumer] = None
  def badJobQueue_=(c: BadJobConsumer) { badJobQueue = Some(c) }

  def apply(codec: JsonCodec): gizzard.scheduler.JobScheduler = {
    val (jobQueue, errorQueue) = schedulerType match {
      case kestrel: KestrelScheduler => {
        val persistentJobQueue = kestrel(jobQueueName)
        val jobQueue = new KestrelJobQueue(jobQueueName, persistentJobQueue, codec)
        val persistentErrorQueue = kestrel(errorQueueName)
        val errorQueue = new KestrelJobQueue(errorQueueName, persistentErrorQueue, codec)

        (jobQueue, errorQueue)
      }

      case memory: MemoryScheduler => {
        val jobQueue = new gizzard.scheduler.MemoryJobQueue(jobQueueName, memory.sizeLimit)
        val errorQueue = new gizzard.scheduler.MemoryJobQueue(errorQueueName, memory.sizeLimit)

        (jobQueue, errorQueue)
      }
    }

    errorQueue.drainTo(jobQueue, errorRetryDelay)

    new gizzard.scheduler.JobScheduler(
      name,
      threads,
      errorStrobeInterval,
      errorLimit,
      perFlushItemLimit,
      jitterRate,
      jobQueue,
      errorQueue,
      badJobQueue.map(_.apply())
    )
  }
}
