package com.twitter.gizzard.jobs

import net.lag.logging.Logger


class JobScheduler(count: Int, val jobQueue: MessageQueue, val errorQueue: MessageQueue) {
  private val log = Logger.get(getClass.getName)
  var executorThreads: Seq[JobEvaluatorThread] = Nil

  def apply(job: Schedulable) {
    jobQueue.put(job)
  }

  @throws(classOf[Exception])
  def retryErrors() {
    errorQueue.writeTo(jobQueue)
  }

  def pauseWork() {
    log.info("Pausing work in JobScheduler: %s", jobQueue)
    jobQueue.pause()
    executorThreads foreach { thread => thread.pauseWork() }
    errorQueue.pause()
  }

  def shutdown() {
    log.info("Shutting down JobScheduler: %s", jobQueue)
    pauseWork()
    jobQueue.shutdown()
    errorQueue.shutdown()
  }

  def isShutdown = jobQueue.isShutdown && errorQueue.isShutdown

  def resumeWork() = {
    log.info("Resuming work in JobScheduler: %s", jobQueue)
    jobQueue.resume()
    errorQueue.resume()
    executorThreads foreach { thread => thread.resumeWork() }
  }

  def size = jobQueue.size

  def start() = {
    log.info("Starting JobScheduler: %s", jobQueue)
    executorThreads = (0 until count).map { _ =>
      new JobEvaluatorThread(new JobEvaluator(jobQueue))
    }.toList
    executorThreads foreach { thread => thread.start() }
  }
}
