package com.twitter.gizzard

import com.twitter.ostrich.StatsProvider
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Config, ConfigMap}
import net.lag.kestrel.{PersistentQueue, QItem}
import net.lag.logging.Logger


class KestrelMessageQueue(queueName: String, config: ConfigMap, jobParser: jobs.JobParser,
                          badJobsLogger: String => Unit, stats: Option[StatsProvider])
  extends MessageQueue {

  val log = Logger.get(getClass.getName)
  val TIMEOUT = 100

  def this(queueName: String, jobProcessor: jobs.JobParser, badJobsLogger: String => Unit) =
    this(queueName, Config.fromMap(Map.empty), jobProcessor, badJobsLogger, None)

  var queue = new PersistentQueue(config("path", "/tmp"), queueName, config)
  queue.setup()

  /** Length (in items) of this queue. */
  def size = queue.length.toInt

  /** Age (in seconds) of items in this queue. */
  def age = queue.currentAge / 1000.0

  def put(value: jobs.Schedulable) = {
    val message = value.toJson
    val startTime = Time.now
    if (!queue.add(message.getBytes)) throw new Exception("Unable to add job to queue")
    stats.map {
      _.addTiming("kestrel-put-timing", (Time.now - startTime).inMillis.toInt)
    }
  }

  def isShutdown = queue.isClosed

  def poll(): Option[QItem] = {
    if (queue.isClosed) None else queue.removeReceive(0, true)
  }

  def get(): Option[QItem] = {
    var item: Option[QItem] = None
    while (item == None && !queue.isClosed) {
      item = queue.removeReceive(System.currentTimeMillis + TIMEOUT, true)
    }
    item
  }

  def process(blocking: Boolean, f: jobs.Job => Unit) = {
    val item = if (blocking) get() else poll()
    item.map { item =>
      val string = new String(item.data)
      try {
        f(jobParser(string))
      } catch {
        case e: Throwable =>
          log.error(e, "Exception parsing job: %s", string)
          badJobsLogger(string)
      } finally {
        queue.confirmRemove(item.xid)
      }
    }
    item
  }

  def foreach(blocking: Boolean, f: jobs.Job => Unit) {
    var item: Option[QItem] = null
    do {
      item = process(blocking, f)
    } while (item.isDefined)
  }

  override def writeTo(messageQueue: MessageQueue) {
    var bound = size
    while (bound > 0) {
      poll() match {
        case None =>
          bound = 0
        case Some(item) =>
          bound -= 1
          val data = new String(item.data)
          log.info("Replaying error job: %s", data)
          messageQueue.put(jobParser(data))
          queue.confirmRemove(item.xid)
      }
    }
  }

  def pause() = {
    queue.pauseReads()
  }

  def resume() = {
    queue.resumeReads()
  }

  def shutdown() = {
    queue.close()
  }

  override def toString() = "<KestrelMessageQueue '%s'>".format(queueName)
}
