package com.twitter.gizzard.scheduler

import scala.collection.Map
import scala.collection.mutable
import net.lag.configgy.ConfigMap


object PrioritizingJobScheduler {
  def apply(config: ConfigMap, codec: Codec, queueNames: Map[Int, String],
                      badJobQueue: Option[JobConsumer]) = {
    val schedulerMap = new mutable.HashMap[Int, JobScheduler]
    queueNames.foreach { case (priority, queueName) =>
      schedulerMap(priority) = JobScheduler(queueName, config, codec, badJobQueue)
    }
    new PrioritizingJobScheduler(schedulerMap)
  }
}

/**
 * A map of JobSchedulers by priority. It can be treated as a single scheduler, and all process
 * operations work on the cluster of schedulers as a whole.
 */
class PrioritizingJobScheduler(val _schedulers: Map[Int, JobScheduler]) extends Process {
  val schedulers = mutable.Map.empty[Int, JobScheduler] ++ _schedulers

  def put(priority: Int, job: JsonJob) {
    apply(priority).put(job)
  }

  def apply(priority: Int): JobScheduler = {
    schedulers.get(priority).getOrElse {
      throw new Exception("No scheduler for priority " + priority)
    }
  }

  def update(priority: Int, scheduler: JobScheduler) {
    schedulers(priority) = scheduler
  }

  def start() = schedulers.values.foreach { _.start() }
  def shutdown() = schedulers.values.foreach { _.shutdown() }
  def isShutdown = schedulers.values.forall { _.isShutdown }
  def pause() = schedulers.values.foreach { _.pause() }
  def resume() = schedulers.values.foreach { _.resume() }
  def retryErrors() = schedulers.values.foreach { _.retryErrors() }

  def size = schedulers.values.foldLeft(0) { _ + _.size }
  def activeThreads = schedulers.values.foldLeft(0) { _ + _.activeThreads }
}
