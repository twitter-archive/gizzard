package com.twitter.gizzard.scheduler

import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.ostrich.{Stats, StatsProvider}
import net.lag.configgy.{Config, ConfigMap}
import net.lag.kestrel.{PersistentQueue, QItem}
import net.lag.logging.Logger


class KestrelMessageQueue(queueName: String, queue: PersistentQueue)
  extends MessageQueue[String, String] {

  val log = Logger.get(getClass.getName)
  val TIMEOUT = 100

  def start() = queue.setup()

  Stats.makeGauge(queueName + "_items") { size }
  Stats.makeGauge(queueName + "_age") { age }

  def size = queue.length.toInt

  /** Age (in seconds) of items in this queue. */
  def age = queue.currentAge / 1000.0

  def put(message: String) = {
    val startTime = Time.now
    if (!queue.add(message.getBytes)) throw new Exception("Unable to add job to queue")
    Stats.addTiming("kestrel-put-timing", (Time.now - startTime).inMillis.toInt)
  }

  def isShutdown = queue.isClosed

  private def poll(): Option[QItem] = {
    if (queue.isClosed) None else queue.removeReceive(0, true)
  }

  private def get(): Option[QItem] = {
    var item: Option[QItem] = None
    while (item == None && !queue.isClosed) {
      // do not use Time.now or it will interact strangely with tests.
      item = queue.removeReceive(System.currentTimeMillis + TIMEOUT, true)
    }
    item
  }

  def elements = new Iterator[String] {
    var itemOption: Option[QItem] = null

    def hasNext = {
      itemOption = get()
      itemOption.isDefined
    }

    def next = {
      val item = itemOption.get
      queue.confirmRemove(item.xid)
      new String(item.data)
    }
  }
  override def writeTo[A](messageQueue: MessageQueue[String, A]) {
    writeTo(messageQueue, size)
  }
  override def writeTo[A](messageQueue: MessageQueue[String, A], limit: Int) {
    var bound = limit
    while (bound > 0) {
      poll() match {
        case None =>
          bound = 0
        case Some(item) =>
          bound -= 1
          val data = new String(item.data)
          log.info("Replaying error job: %s", data)
          messageQueue.put(data)
          queue.confirmRemove(item.xid)
      }
    }
  }

  def pause() = queue.pauseReads()
  def resume() = queue.resumeReads()
  def shutdown() = queue.close()
  override def toString() = "<KestrelMessageQueue '%s'>".format(queueName)
}
