package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import net.lag.logging.Logger
import net.lag.kestrel.{PersistentQueue, PersistentQueueConfig}
import gizzard.scheduler.{JsonJob, Codec, MemoryJobQueue, KestrelJobQueue, JobConsumer}

trait SchedulerType
trait KestrelScheduler extends PersistentQueueConfig with SchedulerType with Cloneable {
  def apply(newName: String): PersistentQueue = {
    // ugh
    val oldName = name
    name = newName
    val q = apply()
    name = oldName
    q
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

  def apply(codec: Codec): gizzard.scheduler.JobScheduler = {
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
