package com.twitter.gizzard.jobs

import scala.collection.Map


class PrioritizingScheduler(schedulers: Map[Int, JobScheduler]) {
  def apply(priority: Int, job: Schedulable) {
    schedulers.get(priority) match {
      case Some(scheduler) => scheduler(job)
      case None => throw new Exception("No scheduler for priority " + priority)
    }
  }

  def apply(priority: Int): JobScheduler = schedulers(priority)

  def start() {
    schedulers.values.foreach { _.start() }
  }

  def shutdown() {
    schedulers.values.foreach { _.shutdown() }
  }

  def isShutdown = schedulers.values.forall { _.isShutdown }

  def pauseWork() {
    schedulers.values.foreach { _.pauseWork() }
  }

  def resumeWork() {
    schedulers.values.foreach { _.resumeWork() }
  }

  def retryErrors() {
    schedulers.values.foreach { _.retryErrors() }
  }

  def size = {
    schedulers.values.foldLeft(0) { _ + _.size }
  }
}
