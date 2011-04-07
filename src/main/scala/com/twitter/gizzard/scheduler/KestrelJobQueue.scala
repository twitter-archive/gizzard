package com.twitter.gizzard
package scheduler

import com.twitter.util.{Duration, Time}
import com.twitter.util.TimeConversions._
import net.lag.kestrel.{PersistentQueue, QItem}
import com.twitter.logging.Logger

/**
 * A JobQueue backed by a kestrel journal file. A codec is used to convert jobs into byte arrays
 * on write, and back into jobs on read. Jobs are not completely removed from the queue until the
 * ticket's 'ack' method is called, so if a job is half-complete when the server dies, it will be
 * back in the queue when the server restarts.
 */
class KestrelJobQueue(queueName: String, val queue: PersistentQueue, codec: JsonCodec)
      extends JobQueue {
  private val log = Logger.get(getClass.getName)
  val TIMEOUT = 100

  Stats.global.addGauge(queueName + "_items") { size }
  Stats.global.addGauge(queueName + "_age") { age }

  def name = queueName
  def size = queue.length.toInt

  /** Age (in seconds) of items in this queue. */
  def age = queue.currentAge.inSeconds

  def start() {
    // don't expire items except when we explicitly call 'discardExpired'.
    queue.config = queue.config.copy(maxExpireSweep = 0)
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

  def put(job: JsonJob) {
    if (!Stats.global.timeMicros("kestrel-put-usec") { queue.add(codec.flatten(job)) }) {
      throw new Exception("Unable to add job to queue")
    }
  }

  def get(): Option[Ticket] = {
    var item: Option[QItem] = None
    while (item == None && !queue.isClosed) {
      // do not use Time.now or it will interact strangely with tests.
      item = queue.removeReceive(Some(Time.fromMilliseconds(System.currentTimeMillis + TIMEOUT)), true)
    }
    item.map { qitem =>
      val decoded = codec.inflate(qitem.data)
      new Ticket {
        def job = decoded
        def ack() {
          queue.confirmRemove(qitem.xid)
        }
        def continue(job: JsonJob) = {
          queue.continue(qitem.xid, codec.flatten(job))
        }
      }
    }
  }

  def drainTo(otherQueue: JobQueue, delay: Duration) {
    require(otherQueue.isInstanceOf[KestrelJobQueue])

    val newConfig = queue.config.copy(maxAge = Some(delay))

    queue.expireQueue = Some(otherQueue.asInstanceOf[KestrelJobQueue].queue)
    queue.config = newConfig
  }

  def checkExpiration(flushLimit: Int) {
    val count = queue.discardExpired(flushLimit)
    if (count > 0) {
//      log.info("Replaying %d error jobs from %s.", count, queueName)
    }
  }

  override def toString() = "<KestrelJobQueue '%s'>".format(queueName)
}
