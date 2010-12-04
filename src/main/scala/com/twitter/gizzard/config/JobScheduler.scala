package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import net.lag.logging.Logger
import net.lag.kestrel.config.PersistentQueue
import gizzard.scheduler.{Job, Codec, MemoryJobQueue, KestrelJobQueue, JobConsumer}

trait SchedulerType
trait KestrelScheduler extends SchedulerType with PersistentQueue {
  def queuePath: String
}
class MemoryScheduler extends SchedulerType {
  var sizeLimit = 0
}

trait BadJobConsumer {
  def apply[J <: Job](): JobConsumer[J]
}

trait JsonJobLogger extends BadJobConsumer {
  var name = "bad_jobs"

  // XXX: this method is not type safe. we need to remove
  //      the type parameter on all of these job related things
  def apply[J <: Job](): JobConsumer[J] =
    new scheduler.JsonJobLogger[scheduler.JsonJob](Logger.get(name)).asInstanceOf[JobConsumer[J]]
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

  def apply[J <: Job](codec: Codec[J]): gizzard.scheduler.JobScheduler[J] = {
    val (jobQueue, errorQueue) = schedulerType match {
      case kestrel: KestrelScheduler => {
        val persistentJobQueue = kestrel(kestrel.queuePath, jobQueueName)
        val jobQueue = new KestrelJobQueue[J](jobQueueName, persistentJobQueue, codec)
        val persistentErrorQueue = kestrel(kestrel.queuePath, errorQueueName)
        val errorQueue = new KestrelJobQueue[J](errorQueueName, persistentErrorQueue, codec)

        (jobQueue, errorQueue)
      }

      case memory: MemoryScheduler => {
        val jobQueue = new gizzard.scheduler.MemoryJobQueue[J](jobQueueName, memory.sizeLimit)
        val errorQueue = new gizzard.scheduler.MemoryJobQueue[J](errorQueueName, memory.sizeLimit)

        (jobQueue, errorQueue)
      }
    }

    errorQueue.drainTo(jobQueue, errorRetryDelay)

    new gizzard.scheduler.JobScheduler[J](
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
