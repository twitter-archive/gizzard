package com.twitter.gizzard.scheduler

import com.twitter.ostrich.{Stats, StatsProvider}
import com.twitter.xrayspecs.{Duration, Time}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.kestrel.{PersistentQueue, QItem}
import net.lag.logging.Logger

/**
 * A JobQueue backed by a kestrel journal file. A codec is used to convert jobs into byte arrays
 * on write, and back into jobs on read. Jobs are not completely removed from the queue until the
 * ticket's 'ack' method is called, so if a job is half-complete when the server dies, it will be
 * back in the queue when the server restarts.
 */
class KestrelJobQueue[J <: Job](queueName: String, val queue: PersistentQueue, codec: Codec[J])
      extends JobQueue[J] {
  private val log = Logger.get(getClass.getName)
  val TIMEOUT = 100

  Stats.makeGauge(queueName + "_items") { size }
  Stats.makeGauge(queueName + "_age") { age }

  def name = queueName
  def size = queue.length.toInt

  /** Age (in seconds) of items in this queue. */
  def age = queue.currentAge / 1000.0

  def start() {
    // don't expire items except when we explicitly call 'discardExpired'.
    queue.maxExpireSweep = 0
    queue.setup()
  }

  def pause() {
    queue.pauseReads()
  }

  def resume() {
    queue.resumeReads()
  }

  def shutdown() {
    queue.close()
  }

  def isShutdown = queue.isClosed

  def put(job: J) {
    if (!Stats.timeMicros("kestrel-put-usec") { queue.add(codec.flatten(job)) }) {
      throw new Exception("Unable to add job to queue")
    }
  }

  def get(): Option[Ticket[J]] = {
    var item: Option[QItem] = None
    while (item == None && !queue.isClosed) {
      // do not use Time.now or it will interact strangely with tests.
      item = queue.removeReceive(System.currentTimeMillis + TIMEOUT, true)
    }
    item.map { qitem =>
      val decoded = codec.inflate(qitem.data)
      new Ticket[J] {
        def job = decoded
        def ack() {
          queue.confirmRemove(qitem.xid)
        }
      }
    }
  }

  def drainTo(otherQueue: JobQueue[J], delay: Duration) {
    queue.expiredQueue.set(Some(Some(otherQueue.asInstanceOf[KestrelJobQueue[J]].queue)))
    queue.maxAge.set(Some(delay.inMilliseconds.toInt))
  }

  def checkExpiration(flushLimit: Int) {
    val count = queue.discardExpired(flushLimit)
    if (count > 0) {
      log.info("Replaying %d error jobs.", count)
    }
  }

  override def toString() = "<KestrelJobQueue '%s'>".format(queueName)
}
