package com.twitter.gizzard.scheduler

import scala.collection.Map
import scala.collection.mutable
import net.lag.configgy.ConfigMap

object PrioritizingJobScheduler {
  def apply[E, J <: Job[E]](config: ConfigMap, codec: Codec[J], queueNames: Map[Int, String], badJobQueue: JobConsumer[J]) = {
    val schedulerMap = new mutable.HashMap[Int, JobScheduler[J]]
    queueNames.foreach { case (priority, queueName) =>
      schedulerMap(priority) = JobScheduler[E, J](queueName, config, codec, badJobQueue)
    }
    new PrioritizingJobScheduler(schedulerMap)
  }
}

class PrioritizingJobScheduler[J <: Job[_]](schedulers: Map[Int, JobScheduler[J]]) extends Process {
  def put(priority: Int, job: J) {
    apply(priority).put(job)
  }

  def apply(priority: Int): JobScheduler[J] = {
    schedulers.get(priority).getOrElse {
      throw new Exception("No scheduler for priority " + priority)
    }
  }

  def start() = schedulers.values.foreach { _.start() }
  def shutdown() = schedulers.values.foreach { _.shutdown() }
  def isShutdown = schedulers.values.forall { _.isShutdown }
  def pause() = schedulers.values.foreach { _.pause() }
  def resume() = schedulers.values.foreach { _.resume() }
  def retryErrors() = schedulers.values.foreach { _.retryErrors() }

  def size = schedulers.values.foldLeft(0) { _ + _.size }
}
