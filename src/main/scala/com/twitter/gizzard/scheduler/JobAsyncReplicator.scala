package com.twitter.gizzard.scheduler

import scala.annotation.tailrec
import com.twitter.util.Time
import com.twitter.gizzard.nameserver.JobRelay
import net.lag.kestrel.config.QueueConfig
import net.lag.kestrel.PersistentQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import com.twitter.logging.Logger


class JobAsyncReplicator(jobRelay: => JobRelay, queueConfig: QueueConfig, queueRootDir: String, threadsPerCluster: Int) {
  private val log = Logger.get(getClass)
  private val exceptionLog = Logger.get("exception")
  private val QueuePollTimeout = 1000 // 1 second
  private val threadpool = Executors.newCachedThreadPool()

  lazy private val queueMap = {
    val m = new ConcurrentHashMap[String, PersistentQueue]
    jobRelay.clusters.foreach { cluster =>
      val queue = new PersistentQueue("replicating_" + cluster, queueRootDir, queueConfig)
      queue.setup()
      for (i <- 0 until threadsPerCluster) {
        threadpool.submit(new Runnable { def run() { process(cluster) } })
      }
      m.put(cluster, queue)
    }
    m
  }

  def enqueue(job: Array[Byte]) {
    jobRelay.clusters.foreach { getQueue(_).add(job) }
  }

  def getQueue(cluster: String) = {
    queueMap.get(cluster) match {
      case null => error("queue not found")
      case queue => queue
    }
  }

  def start() {

  }

  def shutdown() {
    queueMap.values.toArray(Array[PersistentQueue]()).foreach { _.close() }
    if (!threadpool.isShutdown) threadpool.shutdown()
  }

  @tailrec
  final def process(cluster: String) {
    val queue = getQueue(cluster)

    if (!queue.isClosed) {
      try {
        queue.removeReceive(Some(Time.fromMilliseconds(System.currentTimeMillis + QueuePollTimeout)), true) foreach { item =>
          try {
            jobRelay(cluster)(Iterator(item.data).toIterable)
            queue.confirmRemove(item.xid)
          } catch { case e =>
            exceptionLog.error(e, "Exception in job replication for cluster %s: %s", cluster, e.toString)
            queue.unremove(item.xid)
          }
        }
      } catch { case e:java.nio.channels.ClosedByInterruptException =>
        println("Exception: Queue is closed?")
        queue.close()
      }

      process(cluster)
    }
  }
}