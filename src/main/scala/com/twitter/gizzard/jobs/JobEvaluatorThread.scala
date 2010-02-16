package com.twitter.gizzard.jobs

import net.lag.logging.Logger


class JobEvaluatorThread(val executor: JobEvaluator) {
  private val log = Logger.get(getClass.getName)
  @volatile var running = false
  var thread: Thread = null

  private def makeThread() = {
    new Thread("JobEvaluatorThread:" + executor.toString) {
      override def run() {
        running = true
        try {
          executor.apply()
        } catch {
          case e: Throwable =>
            log.error(e, "Exception in JobEvaluatorThread (dying): %s", e.toString)
        }
        log.info("Exiting: JobEvaluatorThread: %s", executor.toString)
        running = false
      }
    }
  }

  def pauseWork() = {
    if (running) {
      log.info("Pausing work in JobEvaluatorThread: %s", executor.toString)
      while (running) {
        Thread.sleep(1)
      }
    }
  }

  def resumeWork() = {
    if (!running) {
      log.info("Resuming work in JobEvaluatorThread: %s", executor.toString)
      start()
      while (!running) {
        Thread.sleep(1)
      }
    }
  }

  def start() = {
    log.info("Starting JobEvaluatorThread: %s", executor.toString)
    thread = makeThread
    thread.start()
    while (!running) {
      Thread.sleep(1)
    }
  }
}
