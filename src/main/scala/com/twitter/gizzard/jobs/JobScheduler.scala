package com.twitter.gizzard.jobs

import com.twitter.ostrich.{BackgroundProcess, StatsProvider}
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger


object JobScheduler {
  /**
   * Configure a JobScheduler from a queue ConfigMap and a scheduler-specific ConfigMap, creating
   * a new ErrorHandlingJobParser and linking the job & error queues together through it.
   */
  def apply(name: String, queueConfig: ConfigMap, jobParser: jobs.JobParser,
            badJobsLogger: String => Unit, stats: Option[StatsProvider]) = {
    val schedulerConfig = queueConfig.configMap(name)
    val errorsConfig =
      new ErrorHandlingConfig(schedulerConfig("error_limit").toInt, badJobsLogger, stats)
    val errorsParser = new ErrorHandlingJobParser(jobParser, errorsConfig)
    val jobQueue = new KestrelMessageQueue(schedulerConfig("job_queue"), queueConfig,
                                           errorsParser, badJobsLogger, stats)
    val errorQueue = new KestrelMessageQueue(schedulerConfig("error_queue"), queueConfig,
                                             errorsParser, badJobsLogger, stats)
    errorsParser.errorQueue = errorQueue
    new JobScheduler(name, schedulerConfig("threads").toInt,
                     schedulerConfig("replay_interval").toInt.seconds, jobQueue, errorQueue)
  }
}

class JobScheduler(val name: String, val threadCount: Int, val replayInterval: Duration,
                   val jobQueue: MessageQueue, val errorQueue: MessageQueue) {
  private val log = Logger.get(getClass.getName)
  var executorThreads: Seq[JobEvaluatorThread] = Nil
  @volatile var running = false

  val retryTask = new BackgroundProcess("Retry process for " + name + " errors") {
    def runLoop() {
      Thread.sleep(replayInterval.inMillis)
      log.info("Replaying %s errors queue...", name)
      try {
        retryErrors()
      } catch {
        case e: Throwable =>
          log.error(e, "Error replaying %s errors!", name)
      }
    }
  }

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
    retryTask.shutdown()
    pauseWork()
    jobQueue.shutdown()
    errorQueue.shutdown()
    running = false
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
    if (!running) {
      running = true
      log.info("Starting JobScheduler: %s", jobQueue)
      executorThreads = (0 until threadCount).map { _ =>
        new JobEvaluatorThread(new JobEvaluator(jobQueue))
      }.toList
      executorThreads foreach { thread => thread.start() }
      retryTask.start()
    }
  }
}
