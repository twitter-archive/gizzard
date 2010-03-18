package com.twitter.gizzard.scheduler

import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger
import net.lag.kestrel.PersistentQueue
import jobs.Schedulable
import com.twitter.ostrich.StatsProvider


object JobScheduler {
  /**
   * Configure a JobScheduler from a queue ConfigMap and a scheduler-specific ConfigMap, creating
   * a new ErrorHandlingJobParser and linking the job & error queues together through it.
   */
  def apply(name: String, queueConfig: ConfigMap, jobParser: jobs.JobParser, stats: StatsProvider) = {
    val schedulerConfig = queueConfig.configMap(name)
    val path = schedulerConfig("path")

    val jobQueueName = schedulerConfig("job_queue")
    val persistentJobQueue = new PersistentQueue(path, jobQueueName, queueConfig)
    val jobQueue = new KestrelMessageQueue(jobQueueName, persistentJobQueue, stats)

    val errorQueueName = schedulerConfig("error_queue")
    val persistentErrorQueue = new PersistentQueue(path, errorQueueName, queueConfig)
    val errorQueue = new KestrelMessageQueue(errorQueueName, persistentErrorQueue, stats)

    val badJobQueue = new JobQueue(new LoggerScheduler(Logger.get("bad_jobs")), jobParser)
    val unparsableMessageQueue = new LoggerScheduler(Logger.get("unparsable_jobs"))

    val errorHandlingConfig = ErrorHandlingConfig(schedulerConfig("replay_interval").toInt.seconds,
                                                  schedulerConfig("error_limit").toInt,
                                                  errorQueue, badJobQueue,
                                                  unparsableMessageQueue, jobParser, stats)
    val errorHandlingJobQueue = new ErrorHandlingJobQueue(name, jobQueue, errorHandlingConfig)
    new JobScheduler(name, schedulerConfig("threads").toInt, errorHandlingJobQueue)
  }
}

class JobScheduler(val name: String, val threadCount: Int, val queue: ErrorHandlingJobQueue)
  extends Process {

  private val log = Logger.get(getClass.getName)
  var executorThreads: Collection[JobEvaluatorThread] = Nil
  @volatile var running = false

  def apply(schedulable: Schedulable) = queue.put(schedulable)
  def retryErrors() = queue.retry()

  def pause() {
    log.info("Pausing work in JobScheduler: %s", queue)
    queue.pause()
    executorThreads foreach { thread => thread.pause() }
  }

  def shutdown() {
    log.info("Shutting down JobScheduler: %s", queue)
    pause()
    queue.shutdown()
    running = false
  }

  def isShutdown = queue.isShutdown

  def resume() = {
    log.info("Resuming work in JobScheduler: %s", queue)
    queue.resume()
    executorThreads foreach { thread => thread.resume() }
  }

  def size = queue.size

  def start() = {
    if (!running) {
      queue.start()
      running = true
      log.info("Starting JobScheduler: %s", queue)
      executorThreads = (0 until threadCount).map { _ =>
        new JobEvaluatorThread(queue)
      }.toList
      executorThreads foreach { thread => thread.start() }
    }
  }
}
