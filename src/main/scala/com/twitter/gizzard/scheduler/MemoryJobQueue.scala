package com.twitter.gizzard.scheduler

import java.util.{ArrayList => JArrayList}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Duration, Time}
import com.twitter.conversions.time._
import com.twitter.logging.Logger


/**
 * A JobQueue that stores the jobs in memory only (in a LinkedBlockingQueue), and may lose jobs
 * if the server dies while jobs are still in the queue. No codec is needed since jobs are passed
 * by reference. This is meant to be used for jobs that can be lost and recovered in some other
 * way, or are of minor importance (like filling a cache).
 */
class MemoryJobQueue(queueName: String, maxSize: Int) extends JobQueue {
  val TIMEOUT = 100

  private val log = Logger.get(getClass.getName)

  Stats.addGauge(queueName + "_items") { size }

  val queue = if (maxSize > 0) {
    new LinkedBlockingQueue[(JsonJob, Time)](maxSize)
  } else {
    new LinkedBlockingQueue[(JsonJob, Time)]
  }

  @volatile var paused = true
  @volatile var running = false

  var expireTo: Option[MemoryJobQueue] = None
  var expireDuration = 0.seconds

  def put(job: JsonJob) {
    while (!queue.offer((job, Time.now + expireDuration))) {
      queue.poll(TIMEOUT, TimeUnit.MILLISECONDS)
      Stats.incr(queueName + "_discarded")
    }
  }

  def innerGet() = {
    var qitem: (JsonJob, Time) = null
    while (running && !paused && (qitem eq null)) {
      qitem = queue.poll(TIMEOUT, TimeUnit.MILLISECONDS)
    }
    if (qitem eq null) {
      None
    } else {
      Some(qitem)
    }
  }

  def get() = {
    innerGet().map { case (item, time) =>
      // no need to ack to the in-memory queue. if the server dies, it's gone.
      new Ticket {
        def job = item
        def ack() { }
        def continue(job: JsonJob) = put(job)
      }
    }
  }

  def drainTo(otherQueue: JobQueue, delay: Duration) {
    expireTo = Some(otherQueue.asInstanceOf[MemoryJobQueue])
    expireDuration = delay
  }

  def checkExpiration(flushLimit: Int) {
    val now = Time.now
    var finished = false
    var count = 0

    while (running && !paused && !finished && count < flushLimit) {
      val qitem = queue.peek()
      if (qitem eq null) {
        finished = true
      } else {
        val (item, time) = qitem
        if (time > now) {
          finished = true
        } else {
          queue.poll()
          expireTo.foreach { _.put(item) }
          count += 1
        }
      }
    }
    if (count > 0) {
      log.info("Replaying %d error jobs.", count)
    }
  }

  def name = queueName

  def size = queue.size

  def start() {
    paused = false
    running = true
    queue.clear()
  }

  def pause() = {
    paused = true
  }

  def resume() {
    paused = false
  }

  def shutdown() {
    paused = true
    running = false
    while (queue.size > 0) {
      log.info("Waiting for %d items to flush before shutting down queue %s", queue.size, queueName)
      Thread.sleep(1000)
    }
  }

  def isShutdown = !running
}
