package com.twitter.gizzard.scheduler

import scala.collection.Map
import jobs.Job


class PrioritizingJobScheduler(schedulers: Map[Int, JobScheduler]) extends Process {
  def apply(priority: Int, job: Job) {
    schedulers.get(priority) match {
      case Some(scheduler) => scheduler(job)
      case None => throw new Exception("No scheduler for priority " + priority)
    }
  }

  def apply(priority: Int): JobScheduler = schedulers(priority)

  def start() = schedulers.values.foreach { _.start() }
  def shutdown() = schedulers.values.foreach { _.shutdown() }
  def isShutdown = schedulers.values.forall { _.isShutdown }
  def pause() = schedulers.values.foreach { _.pause() }
  def resume() = schedulers.values.foreach { _.resume() }
  def retryErrors() = schedulers.values.foreach { _.retryErrors() }

  def size = schedulers.values.foldLeft(0) { _ + _.size }
}
