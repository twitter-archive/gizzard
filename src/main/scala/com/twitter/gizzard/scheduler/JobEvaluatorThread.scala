package com.twitter.gizzard.scheduler

import net.lag.logging.Logger
import jobs.{Schedulable, Job}


class JobEvaluatorThread(jobQueue: Collection[Job]) extends Process {
  private val log = Logger.get(getClass.getName)
  @volatile private var running = false
  private var thread: Thread = null

  private def makeThread() = {
    new Thread("JobEvaluatorThread:" + jobQueue.toString) {
      override def run() {
        running = true
        try {
          jobQueue.foreach(_.apply())
        } catch {
          case e: Throwable =>
            log.error(e, "Exception in JobEvaluatorThread (dying): %s", e.toString)
        }
        log.info("Exiting: JobEvaluatorThread: %s", jobQueue.toString)
        running = false
      }
    }
  }

  def isShutdown = !running

  def pause() {
    if (running) {
      log.info("Pausing work in JobEvaluatorThread: %s", jobQueue.toString)
      while (running) Thread.sleep(1)
    }
  }

  def shutdown() = pause()

  def resume() {
    if (!running) {
      log.info("Resuming work in JobEvaluatorThread: %s", jobQueue.toString)
      start()
      while (!running) Thread.sleep(1)
    }
  }

  def start() {
    log.info("Starting JobEvaluatorThread: %s", jobQueue.toString)
    thread = makeThread
    thread.start()
    while (!running) Thread.sleep(1)
  }
}
