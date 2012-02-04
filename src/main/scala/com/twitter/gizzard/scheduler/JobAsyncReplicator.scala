package com.twitter.gizzard.scheduler

import scala.annotation.tailrec
import com.twitter.util.Time
import com.twitter.gizzard.nameserver.JobRelay
import net.lag.kestrel.config.QueueConfig
import net.lag.kestrel.PersistentQueue
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.Executors
import com.twitter.logging.Logger


class JobAsyncReplicator(jobRelay: => JobRelay, queueConfig: QueueConfig, queueRootDir: String, threadsPerCluster: Int) {

  // TODO make configurable
  private val WatchdogPollInterval = 100 // 100 millis
  private val QueuePollTimeout    = 1000 // 1 second

  @volatile private var queueMap: Map[String, PersistentQueue] = Map()
  @volatile private var watchdogThread: Option[Thread]         = None

  private val log          = Logger.get(getClass)
  private val exceptionLog = Logger.get("exception")

  private val threadpool = Executors.newCachedThreadPool()

  def clusters = queueMap.keySet
  def queues   = queueMap.values.toSeq

  def enqueue(job: Array[Byte]) {
    queues foreach { _.add(job) }
  }

  def start() {
    watchdogThread = Some(newWatchdogThread)
    watchdogThread foreach { _.start() }
  }

  def shutdown() {
    watchdogThread foreach { _.interrupt() }
    queues foreach { _.close() }
    if (!threadpool.isShutdown) threadpool.shutdown()
  }

  private final def process(cluster: String) {
    ignoreInterrupt {
      queueMap.get(cluster) foreach { queue =>

        while (!queue.isClosed) {
          queue.removeReceive(removeTimeout, true) foreach { item =>
            try {
              jobRelay(cluster)(Seq(item.data))
              queue.confirmRemove(item.xid)
            } catch { case e =>
              exceptionLog.error(e, "Exception in job replication for cluster %s: %s", cluster, e.toString)
              queue.unremove(item.xid)
            }
          }
        }
      }
    }
  }

  def reconfigure() {
    // save the relay so we work on a single instance in case it is reconfigured again.
    val r = jobRelay

    if (r.clusters != clusters) {
      queues foreach { _.close() }

      val qs = (r.clusters foldLeft Map[String, PersistentQueue]()) { (m, c) =>
        m + (c -> newQueue(c))
      }

      queueMap = qs

      qs.values foreach { _.setup }

      for (c <- qs.keys; i <- 0 until threadsPerCluster) {
        threadpool.submit(newProcessor(c))
      }
    }

    // wait before returning
    Thread.sleep(WatchdogPollInterval)
  }


  // Helpers

  private def removeTimeout = {
    Some(Time.fromMilliseconds(System.currentTimeMillis + QueuePollTimeout))
  }

  private def newQueue(cluster: String) = {
    new PersistentQueue("replicating_" + cluster, queueRootDir, queueConfig)
  }

  private def newProcessor(cluster: String) = {
    new Runnable { def run() { process(cluster) } }
  }

  private def ignoreInterrupt[A](f: => A) = try {
    f
  } catch {
    case e: InterruptedException       => ()
    case e: ClosedByInterruptException => ()
  }

  private def newWatchdogThread = new Thread {
    override def run() {
      ignoreInterrupt {
        while (!isInterrupted) reconfigure()
      }
    }
  }
}
