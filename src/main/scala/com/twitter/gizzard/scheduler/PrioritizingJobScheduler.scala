package com.twitter.gizzard.scheduler

import scala.collection.Map
import jobs.Schedulable


class PrioritizingJobScheduler(schedulers: Map[Int, JobScheduler]) extends Process {
  def apply(priority: Int, schedulable: Schedulable) {
    schedulers.get(priority) match {
      case Some(scheduler) => scheduler(schedulable)
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
