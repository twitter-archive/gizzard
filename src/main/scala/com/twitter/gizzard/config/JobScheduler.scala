package com.twitter.gizzard.config

import com.twitter.util.Duration
import net.lag.logging.Logger
import net.lag.kestrel.config.PersistentQueue
import gizzard.scheduler.{Job, Codec, MemoryJobQueue, KestrelJobQueue, JobConsumer}

trait SchedulerType
trait Kestrel extends SchedulerType with PersistentQueue {
  def queuePath: String
}
trait Memory extends SchedulerType {
  def sizeLimit: Int = 0
}

trait BadJobConsumer {
  def apply[J <: Job](): JobConsumer[J]
}

trait JsonJobLogger extends BadJobConsumer {
  def name: String

  // XXX: this method is not type safe. we need to remove
  //      the type parameter on all of these job related things
  def apply[J <: Job](): JobConsumer[J] =
    new scheduler.JsonJobLogger[scheduler.JsonJob](Logger.get(name)).asInstanceOf[JobConsumer[J]]
}

trait Scheduler {
  def schedulerType: SchedulerType
  def threads: Int
  def errorStrobeInterval: Duration
  def errorRetryDelay: Duration
  def perFlushItemLimit: Int
  def jitterRate: Float
  def errorLimit: Int
  def name: String
  def jobQueueName: String = name
  def errorQueueName: String = name + "_errors"
  def badJobQueue : Option[BadJobConsumer]

  def apply[J <: Job](codec: Codec[J]): gizzard.scheduler.JobScheduler[J] = {
    val (jobQueue, errorQueue) = schedulerType match {
      case kestrel: Kestrel => {
        val persistentJobQueue = kestrel(kestrel.queuePath, jobQueueName)
        val jobQueue = new KestrelJobQueue[J](jobQueueName, persistentJobQueue, codec)
        val persistentErrorQueue = kestrel(kestrel.queuePath, errorQueueName)
        val errorQueue = new KestrelJobQueue[J](errorQueueName, persistentErrorQueue, codec)

        (jobQueue, errorQueue)
      }

      case memory: Memory => {
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

trait JobScheduler {
  def path: String = "/var/spool/kestrel"
}
