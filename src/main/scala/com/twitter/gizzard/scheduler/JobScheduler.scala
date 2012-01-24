package com.twitter.gizzard.scheduler

import java.util.Random
import java.util.concurrent.atomic.AtomicInteger
import net.lag.kestrel.PersistentQueue
import com.twitter.ostrich.admin.BackgroundProcess
import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.gizzard.Stats
import com.twitter.gizzard.shards.{ShardBlackHoleException, ShardOfflineException}
import com.twitter.gizzard.nameserver.JobRelay
import com.twitter.gizzard.util.Process


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
class JobScheduler(
  val name: String,
  val threadCount: Int,
  val strobeInterval: Duration,
  val errorLimit: Int,
  val flushLimit: Int,
  val jitterRate: Float,
  val isReplicated: Boolean,
  jobRelay: => JobRelay,
  val queue: JobQueue,
  val errorQueue: JobQueue,
  val badJobQueue: JobConsumer)
extends Process with JobConsumer {

  private val log = Logger.get(getClass.getName)
  private val exceptionLog = Logger.get("exception")
  var workerThreads: Iterable[BackgroundProcess] = Nil
  @volatile var running = false
  private var _activeThreads = new AtomicInteger

  def activeThreads = _activeThreads.get()

  val retryTask = new BackgroundProcess("Retry process for " + name + " errors") {
    def runLoop() {
      val jitter = math.round(strobeInterval.inMillis * jitterRate * (2*new Random().nextDouble() - 1))
      Thread.sleep(strobeInterval.inMillis + jitter)
      try {
        checkExpiredJobs()
      } catch {
        case e: Throwable =>
          exceptionLog.error(e, "Error replaying %s errors", name)
//          log.error(e, "Error replaying %s errors!", name)
      }
    }
  }

  def retryErrors() {
    var bound = errorQueue.size
    while (bound > 0) {
      errorQueue.get() match {
        case None    => bound = 0
        case Some(t) => {
          queue.put(t.job)
          t.ack()
          bound -= 1
        }
      }
    }
  }

  def checkExpiredJobs() {
    errorQueue.checkExpiration(flushLimit)
  }

  def addFanout(suffix: String) { queue.addFanout(suffix) }
  def removeFanout(suffix: String) { queue.removeFanout(suffix) }
  def listFanout() = { queue.listFanout() }

  def start() = {
    if (!running) {
      queue.start()
      errorQueue.start()
      running = true
      log.debug("Starting JobScheduler: %s", queue)
      workerThreads = (0 until threadCount).map { makeWorker(_) }.toList
      workerThreads.foreach { _.start() }
      retryTask.start()
    }
  }

  def pause() {
    log.debug("Pausing work in JobScheduler: %s", queue)
    queue.pause()
    errorQueue.pause()
    shutdownWorkerThreads()
  }

  def resume() = {
    log.debug("Resuming work in JobScheduler: %s", queue)
    queue.resume()
    errorQueue.resume()
    workerThreads = (0 until threadCount).map { makeWorker(_) }.toList
    workerThreads.foreach { _.start() }
  }

  def shutdown() {
    if(running) {
      log.debug("Shutting down JobScheduler: %s", queue)
      queue.shutdown()
      errorQueue.shutdown()
      shutdownWorkerThreads()
      retryTask.shutdown()
      running = false
    }
  }

  private def shutdownWorkerThreads() {
    workerThreads.foreach { _.shutdown() }
    workerThreads = Nil
  }

  def isShutdown = queue.isShutdown

  def put(job: JsonJob) {
    queue.put(job)
  }

  def size = queue.size
  def errorSize = errorQueue.size

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
    queue.get.foreach { ticket =>
      _activeThreads.incrementAndGet()
      try {
        val job = ticket.job
        try {
          if (isReplicated && job.shouldReplicate) jobRelay.enqueue(job.toJsonBytes)
          // TODO(Abhi Khune): 
          // job.shouldReplicate = false
          job()
          Stats.incr("job-success-count")
        } catch {
          case e: ShardBlackHoleException => Stats.incr("job-blackholed-count")
          case e: ShardOfflineException =>
            Stats.incr("job-blocked-count")
            errorQueue.put(job)
          case e =>
            Stats.incr("job-error-count")
            exceptionLog.error(e, "Job: %s", job)
//            log.error(e, "Error in Job: %s - %s", job, e)
            job.errorCount += 1
            job.errorMessage = e.toString
            if (job.errorCount > errorLimit) {
              badJobQueue.put(job)
            } else {
              errorQueue.put(job)
            }
        }
        job.nextJob match {
          case None => ticket.ack()
          case _    => ticket.continue(job.nextJob.get)
        }
      } finally {
        _activeThreads.decrementAndGet()
      }
    }
  }
}
