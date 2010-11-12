package com.twitter.gizzard.scheduler

import java.util.{ArrayList => JArrayList}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.collection.jcl
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.{Duration, Time}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger

/**
 * A JobQueue that stores the jobs in memory only (in a LinkedBlockingQueue), and may lose jobs
 * if the server dies while jobs are still in the queue. No codec is needed since jobs are passed
 * by reference. This is meant to be used for jobs that can be lost and recovered in some other
 * way, or are of minor importance (like filling a cache).
 */
class MemoryJobQueue[J <: Job](queueName: String, maxSize: Int) extends JobQueue[J] {
  val TIMEOUT = 100

  private val log = Logger.get(getClass.getName)

  Stats.makeGauge(queueName + "_items") { size }

  val queue = if (maxSize > 0) {
    new LinkedBlockingQueue[(J, Time)](maxSize)
  } else {
    new LinkedBlockingQueue[(J, Time)]
  }

  @volatile var paused = true
  @volatile var running = false

  var expireTo: Option[MemoryJobQueue[J]] = None
  var expireDuration = 0.seconds

  def put(job: J) {
    while (!queue.offer((job, Time.now + expireDuration))) {
      queue.poll(TIMEOUT, TimeUnit.MILLISECONDS)
      Stats.incr(queueName + "_discarded")
    }
  }

  def innerGet() = {
    var qitem: (J, Time) = null
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
      new Ticket[J] {
        def job = item
        def ack() { }
      }
    }
  }

  def drainTo(otherQueue: JobQueue[J], delay: Duration) {
    expireTo = Some(otherQueue.asInstanceOf[MemoryJobQueue[J]])
    expireDuration = delay
  }

  def checkExpiration() {
    val now = Time.now
    var finished = false
    var count = 0

    while (running && !paused && !finished) {
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
