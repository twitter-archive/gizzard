package com.twitter.gizzard.scheduler

import com.twitter.ostrich.{BackgroundProcess, Stats}
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.kestrel.PersistentQueue
import net.lag.logging.Logger
import shards.ShardRejectedOperationException

object JobScheduler {
  /**
   * Configure a JobScheduler from a queue ConfigMap and a scheduler-specific ConfigMap, creating
   * a new ErrorHandlingJobParser and linking the job & error queues together through it.
   */
  def apply[J <: Job](name: String, queueConfig: ConfigMap, codec: Codec[J], badJobQueue: JobConsumer[J]) = {
    val path = queueConfig("path")
    val schedulerConfig = queueConfig.configMap(name)

    val jobQueueName = schedulerConfig("job_queue")
    val persistentJobQueue = new PersistentQueue(path, jobQueueName, queueConfig)
    val jobQueue = new KestrelJobQueue[J](jobQueueName, persistentJobQueue, codec)

    val errorQueueName = schedulerConfig("error_queue")
    val persistentErrorQueue = new PersistentQueue(path, errorQueueName, queueConfig)
    val errorQueue = new KestrelJobQueue[J](errorQueueName, persistentErrorQueue, codec)

    val threadCount = schedulerConfig("threads").toInt
    val retryInterval = schedulerConfig("replay_interval").toInt.seconds
    val errorLimit = schedulerConfig("error_limit").toInt

    new JobScheduler[J](name, threadCount, retryInterval, errorLimit, jobQueue, errorQueue, badJobQueue)
  }
}

/**
 * A cluster of worker threads which poll a JobQueue for work and execute jobs.
 *
 * If a job throws an exception, its error count is incremented. If the error count exceeds
 * 'errorLimit', the job is written into 'badJobQueue', which is usually some sort of log file.
 * Otherwise, the job is written into 'errorQueue'.
 *
 * At regular intervals specified by 'retryInterval', 'errorQueue' is drained back into 'queue'
 * to give erroring jobs another chance to run.
 *
 * Jobs are added to the scheduler with 'put', and the thread pool & queues can be controlled
 * with the normal Process methods ('start', 'shutdown', and so on).
 */
class JobScheduler[J <: Job](val name: String,
                             val threadCount: Int,
                             val retryInterval: Duration,
                             val errorLimit: Int,
                             val queue: JobQueue[J],
                             val errorQueue: JobQueue[J],
                             val badJobQueue: JobConsumer[J])
      extends Process with JobConsumer[J] {

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
      retryTask.start()
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
    retryTask.shutdown()
    running = false
  }

  def isShutdown = queue.isShutdown

  def put(job: J) {
    queue.put(job)
  }

  def size = queue.size

  private def makeWorker(n: Int) = {
    new BackgroundProcess("JobEvaluatorThread:" + name + ":" + n.toString) {
      def runLoop() {
        processWork()
      }
    }
  }

  // hook to let unit tests stub out threads.
  protected def processWork() {
    process()
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
