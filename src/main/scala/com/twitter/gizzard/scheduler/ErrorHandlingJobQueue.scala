package com.twitter.gizzard.scheduler

import com.twitter.ostrich.BackgroundProcess
import com.twitter.ostrich.StatsProvider
import com.twitter.xrayspecs.Duration
import net.lag.logging.Logger
import java.util.Random
import jobs.{Schedulable, Job, JobParser, ErrorHandlingJobParser, UnparsableJobException}


case class ErrorHandlingConfig(retryInterval: Duration, errorLimit: Int,
                               perFlushItemLimit: Int, jitterRate: Float,
                               errorQueue: MessageQueue[String, String],
                               badJobQueue: Scheduler[Schedulable],
                               unparsableMessageQueue: Scheduler[String],
                               jobParser: JobParser)


class ErrorHandlingJobQueue(name: String, val normalQueue: MessageQueue[String, String],
                            config: ErrorHandlingConfig)
  extends Collection[Job] with Scheduler[Schedulable] with Process {

  val (retryInterval, errorQueue, unparsableMessageQueue, jobParser) =
    (config.retryInterval, config.errorQueue, config.unparsableMessageQueue, config.jobParser)
  val normalJobQueue = new JobQueue(normalQueue, jobParser)
  val errorJobQueue = new JobQueue(errorQueue, jobParser)
  val errorHandlingJobParser = new ErrorHandlingJobParser(config, errorJobQueue)
  val log = Logger.get(getClass.getName)
  val perFlushLimit = config.perFlushItemLimit

  val retryTask = new BackgroundProcess("Retry process for " + name + " errors") {
    def runLoop() {
      val jitter = Math.round(retryInterval.inMillis * config.jitterRate * new Random().nextGaussian())
      Thread.sleep(retryInterval.inMillis + jitter)
      try {
        retry()
      } catch {
        case e: Throwable =>
          log.error(e, "Error replaying %s errors!", name)
      }
    }
  }

  def retry() {
    log.info("Replaying %s errors queue...", name)
    errorQueue.writeTo(normalQueue, perFlushLimit)
  }

  def put(schedulable: Schedulable) = normalJobQueue.put(schedulable)

  def putError(schedulable: Schedulable) = errorJobQueue.put(schedulable)

  def start() {
    normalQueue.start()
    errorQueue.start()
    retryTask.start()
  }

  def pause() {
    normalQueue.pause()
    errorQueue.pause()
  }

  def resume() {
    normalQueue.resume()
    errorQueue.resume()
  }

  def shutdown() {
    retryTask.shutdown()
    normalQueue.shutdown()
    errorQueue.shutdown()
  }

  def isShutdown = normalQueue.isShutdown && errorQueue.isShutdown
  def size = normalQueue.size

  def elements = new Iterator[Job] {
    val normalElements = normalQueue.elements
    var element: Job = null
    def hasNext: Boolean = {
      if (!normalElements.hasNext) return false

      val message = normalElements.next
      try {
        element = errorHandlingJobParser(message)
      } catch {
        case e: UnparsableJobException =>
          log.error(e, "Error parsing job!")
          unparsableMessageQueue.put(message)
          return hasNext
      }
      element != null
    }

    def next = element
  }

  override def toString() = "<ErrorHandlingJobQueue '%s'>".format(name)
}
