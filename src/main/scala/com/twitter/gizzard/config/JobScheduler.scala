package com.twitter.gizzard.config

import com.twitter.util.Duration
import net.lag.kestrel.config.PersistentQueue
import gizzard.scheduler.{Job, Codec, MemoryJobQueue, KestrelJobQueue, JobConsumer}

trait SchedulerType
trait Kestrel extends SchedulerType with PersistentQueue {
  def queuePath: String
}
trait Memory extends SchedulerType {
  def sizeLimit: Int = 0
}

trait Scheduler {
  def schedulerType: SchedulerType
  def threads: Int
  def replayInterval: Duration
  def perFlushItemLimit: Int
  def jitterRate: Float
  def errorLimit: Int
  def name: String
  def jobQueueName: String = name
  def errorQueueName: String = name + "_errors"

  def apply[J <: Job](codec: Codec[J], badJobQueue: Option[JobConsumer[J]]): gizzard.scheduler.JobScheduler[J] = {
    schedulerType match {
      case kestrel: Kestrel =>
        val persistentJobQueue = kestrel(kestrel.queuePath, jobQueueName)
        val jobQueue = new KestrelJobQueue[J](jobQueueName, persistentJobQueue, codec)
        val persistentErrorQueue = kestrel(kestrel.queuePath, errorQueueName)
        val errorQueue = new KestrelJobQueue[J](errorQueueName, persistentErrorQueue, codec)

        new gizzard.scheduler.JobScheduler[J](name, threads, replayInterval, errorLimit, perFlushItemLimit, jitterRate, jobQueue, errorQueue, badJobQueue)
      case memory: Memory =>
        val jobQueue = new gizzard.scheduler.MemoryJobQueue[J](jobQueueName, memory.sizeLimit)
        val errorQueue = new gizzard.scheduler.MemoryJobQueue[J](errorQueueName, memory.sizeLimit)
        new gizzard.scheduler.JobScheduler[J](name, threads, replayInterval, errorLimit, perFlushItemLimit, jitterRate, jobQueue, errorQueue, badJobQueue)
    }
  }
}

trait JobScheduler {
  def path: String = "/var/spool/kestrel"
}
