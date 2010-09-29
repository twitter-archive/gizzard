package com.twitter.gizzard.scheduler

import java.util.{ArrayList => JArrayList}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.collection.jcl
import com.twitter.ostrich.Stats

/**
 * A JobQueue that stores the jobs in memory only (in a LinkedBlockingQueue), and may lose jobs
 * if the server dies while jobs are still in the queue. No codec is needed since jobs are passed
 * by reference. This is meant to be used for jobs that can be lost and recovered in some other
 * way, or are of minor importance (like filling a cache).
 */
class MemoryJobQueue[J <: Job](queueName: String, maxSize: Int) extends JobQueue[J] {
  val TIMEOUT = 100

  Stats.makeGauge(queueName + "_items") { size }

  val queue = if (maxSize > 0) {
    new LinkedBlockingQueue[J](maxSize)
  } else {
    new LinkedBlockingQueue[J]
  }

  @volatile var paused = true
  @volatile var running = false

  def put(job: J) {
    while (!queue.offer(job)) {
      queue.poll(TIMEOUT, TimeUnit.MILLISECONDS)
      Stats.incr(queueName + "_discarded")
    }
  }

  def get() = {
    var item: J = null.asInstanceOf[J]
    while (running && !paused && (item eq null)) {
      item = queue.poll(TIMEOUT, TimeUnit.MILLISECONDS)
    }
    if (item eq null) {
      None
    } else {
      // no need to ack to the in-memory queue. if the server dies, it's gone.
      Some(new Ticket[J] {
        def job = item
        def ack() { }
      })
    }
  }

  def drainTo(otherQueue: JobQueue[J]) {
    if (running && !paused) {
      val collection = new JArrayList[J]
      queue.drainTo(collection)
      jcl.Buffer(collection).foreach { otherQueue.put(_) }
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
    queue.clear()
  }

  def isShutdown = !running
}
