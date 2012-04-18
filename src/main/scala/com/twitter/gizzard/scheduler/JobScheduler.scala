/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.gizzard.scheduler

import java.lang.reflect.UndeclaredThrowableException
import java.util.Random
import java.util.concurrent.ExecutionException
import com.twitter.ostrich.{BackgroundProcess, Stats}
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.kestrel.PersistentQueue
import net.lag.logging.Logger
import shards.{NormalShardException, ShardRejectedOperationException}

object JobScheduler {
  /**
   * Configure a JobScheduler from a queue ConfigMap and a scheduler-specific ConfigMap, creating
   * a new ErrorHandlingJobParser and linking the job & error queues together through it.
   */
  def apply[J <: Job](name: String, queueConfig: ConfigMap, codec: Codec[J],
                      badJobQueue: Option[JobConsumer[J]]) = {
    val path = queueConfig.getString("path", "/var/spool/kestrel")
    val schedulerConfig = queueConfig.configMap(name)

    val threadCount = schedulerConfig("threads").toInt
    val strobeInterval = schedulerConfig("strobe_interval").toInt.milliseconds
    val errorLimit = schedulerConfig("error_limit").toInt
    val flushLimit = schedulerConfig("flush_limit").toInt
    val errorDelay = schedulerConfig("error_delay").toInt.seconds
    val sizeLimit = schedulerConfig.getInt("size_limit", 0)
    val jitterRate = schedulerConfig("jitter_rate").toFloat

    val jobQueueName = schedulerConfig("job_queue")
    val errorQueueName = schedulerConfig("error_queue")

    schedulerConfig.getString("type", "kestrel") match {
      case "kestrel" =>
        val persistentJobQueue = new PersistentQueue(path, jobQueueName, queueConfig)
        val jobQueue = new KestrelJobQueue[J](jobQueueName, persistentJobQueue, codec)

        val persistentErrorQueue = new PersistentQueue(path, errorQueueName, queueConfig)
        val errorQueue = new KestrelJobQueue[J](errorQueueName, persistentErrorQueue, codec)
        errorQueue.drainTo(jobQueue, errorDelay)

        new JobScheduler[J](name, threadCount, strobeInterval, errorLimit, flushLimit,
                            jitterRate, jobQueue, errorQueue, badJobQueue)

      case "memory" =>
        val jobQueue = new MemoryJobQueue[J](jobQueueName, sizeLimit)
        val errorQueue = new MemoryJobQueue[J](errorQueueName, sizeLimit)
        errorQueue.drainTo(jobQueue, errorDelay)
        new JobScheduler[J](name, threadCount, strobeInterval, errorLimit, flushLimit,
                            jitterRate, jobQueue, errorQueue, badJobQueue)

      case x =>
        throw new Exception("Unknown queue type " + x)
    }
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
                             val strobeInterval: Duration,
                             val errorLimit: Int,
                             val flushLimit: Int,
                             val jitterRate: Float,
                             val queue: JobQueue[J],
                             val errorQueue: JobQueue[J],
                             val badJobQueue: Option[JobConsumer[J]])
      extends Process with JobConsumer[J] {

  private val log = Logger.get(getClass.getName)
  var workerThreads: Collection[BackgroundProcess] = Nil
  @volatile var running = false

  val retryTask = new BackgroundProcess("Retry process for " + name + " errors") {
    def runLoop() {
      val jitter = Math.round(strobeInterval.inMillis * jitterRate * new Random().nextGaussian())
      Thread.sleep(strobeInterval.inMillis + jitter)
      try {
        retryErrors()
      } catch {
        case e: Throwable =>
          log.error(e, "Error replaying %s errors!", name)
      }
    }
  }

  def retryErrors() {
    errorQueue.checkExpiration(flushLimit)
  }

  def start() = {
    if (!running) {
      queue.start()
      errorQueue.start()
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
    errorQueue.pause()
    workerThreads.foreach { _.shutdown() }
    workerThreads = Nil
  }

  def resume() = {
    log.info("Resuming work in JobScheduler: %s", queue)
    queue.resume()
    errorQueue.resume()
    workerThreads = (0 until threadCount).map { makeWorker(_) }.toList
    workerThreads.foreach { _.start() }
  }

  def shutdown() {
    try {
      log.info("Shutting down JobScheduler: %s", queue)
      queue.shutdown()
      retryTask.shutdown()
      workerThreads.foreach { _.shutdown() }
      workerThreads = Nil
      errorQueue.shutdown()
      running = false
    } catch {
      case e: Throwable =>
        log.error(e, "Failed to shutdown %s", queue)
        throw e
    }
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

  private def unwrapException(exception: Throwable): Throwable = {
    exception match {
      case e: ExecutionException =>
        unwrapException(e.getCause)
      case e: UndeclaredThrowableException =>
        // fondly known as JavaOutrageException
        unwrapException(e.getCause)
      case e =>
        e
    }
  }

  // hook to let unit tests stub out threads.
  protected def processWork() {
    process()
  }

  def process() {
    (try {
      queue.get()
    } catch {
      case e: Exception =>
        log.error(e, "Unable to parse job")
        None
    }).foreach { ticket =>
      val job = ticket.job
      try {
        job()
        Stats.incr("job-success-count")
      } catch {
        case e: Throwable =>
          unwrapException(e) match {
            case e: ShardRejectedOperationException =>
              Stats.incr("job-darkmoded-count")
              errorQueue.put(job)
            case e: NormalShardException =>
              Stats.incr("job-error-count")
              log.error("Error in Job: %s - %s", job, e)
              errorQueue.put(job)
            case e =>
              Stats.incr("job-error-count")
              log.error(e, "Error in Job: %s - %s", job, e)
              job.errorCount += 1
              job.errorMessage = e.toString
              if (job.errorCount > errorLimit) {
                badJobQueue.foreach { _.put(job) }
              } else {
                errorQueue.put(job)
              }
          }
      }
      ticket.ack()
    }
  }
}
