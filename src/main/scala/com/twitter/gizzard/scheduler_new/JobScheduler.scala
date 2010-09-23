package com.twitter.gizzard.scheduler_new

import com.twitter.ostrich.{BackgroundProcess, Stats}
import com.twitter.xrayspecs.Duration
import net.lag.logging.Logger
import shards.ShardRejectedOperationException

class JobScheduler[E, J <: Job[E]](val name: String,
                                   val threadCount: Int,
                                   val retryInterval: Duration,
                                   val errorLimit: Int,
                                   val queue: JobQueue[E, J],
                                   val errorQueue: JobQueue[E, J],
                                   val badJobQueue: JobConsumer[E, J])
      extends Process with JobConsumer[E, J] {

  private val log = Logger.get(getClass.getName)
  var workerThreads: Collection[BackgroundProcess] = Nil
  @volatile var running = false

  val retryTask = new BackgroundProcess("Retry process for " + name + " errors") {
    def runLoop() {
      Thread.sleep(retryInterval.inMillis)
      try {
        retryErrors()
      } catch {
        case e: Throwable =>
          log.error(e, "Error replaying %s errors!", name)
      }
    }
  }

  def retryErrors() {
    log.info("Replaying %s errors queue...", name)
    errorQueue.drainTo(queue)
  }

  def start() = {
    if (!running) {
      queue.start()
      running = true
      log.info("Starting JobScheduler: %s", queue)
      workerThreads = (0 until threadCount).map { makeWorker(_) }.toList
      workerThreads.foreach { _.start() }
    }
  }

  def pause() {
    log.info("Pausing work in JobScheduler: %s", queue)
    queue.pause()
    workerThreads.foreach { _.shutdown() }
    workerThreads = Nil
  }

  def resume() = {
    log.info("Resuming work in JobScheduler: %s", queue)
    queue.resume()
    workerThreads = (0 until threadCount).map { makeWorker(_) }.toList
    workerThreads.foreach { _.start() }
  }

  def shutdown() {
    log.info("Shutting down JobScheduler: %s", queue)
    pause()
    queue.shutdown()
    running = false
  }

  def isShutdown = queue.isShutdown

  def put(job: J) {
    queue.put(job)
  }

  private def makeWorker(n: Int) = {
    new BackgroundProcess("JobEvaluatorThread:" + name + ":" + n.toString) {
      def runLoop() {
        process()
      }
    }
  }

  def process() {
    queue.get().foreach { ticket =>
      val job = ticket.job
      try {
        job()
        Stats.incr("job-success-count")
      } catch {
        case e: ShardRejectedOperationException =>
          Stats.incr("job-darkmoded-count")
          errorQueue.put(job)
        case e =>
          Stats.incr("job-error-count")
          log.error(e, "Error in Job: " + e)
          job.errorCount += 1
          job.errorMessage = e.toString
          if (job.errorCount > errorLimit) {
            badJobQueue.put(job)
          } else {
            errorQueue.put(job)
          }
      }
      ticket.ack()
    }
  }
}
