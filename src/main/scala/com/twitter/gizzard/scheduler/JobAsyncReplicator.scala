package com.twitter.gizzard.scheduler

import scala.annotation.tailrec
import com.twitter.util.Time
import com.twitter.gizzard.nameserver.JobRelay
import net.lag.kestrel.config.QueueConfig
import net.lag.kestrel.PersistentQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

class JobAsyncReplicator(jobRelay: => JobRelay, queueConfig: QueueConfig, queueRootDir: String, threadsPerCluster: Int) {

  private val QueuePollTimeout = 1000 // 1 second

  lazy val queueMap = {
    val m = new ConcurrentHashMap[String, PersistentQueue]
    jobRelay.clusters.foreach { cluster =>
      val queue = new PersistentQueue("replicating_" + cluster, queueRootDir, queueConfig)
      queue.setup()
      m.put(cluster, queue)
    }
    m
  }
  val threadpool = Executors.newCachedThreadPool()

  def enqueue(job: Array[Byte]) {
    jobRelay.clusters.foreach { getQueue(_).add(job) }
  }

  def getQueue(cluster: String) = {
    queueMap.get(cluster) match {
      case null => {
        // if (null == queueMap.putIfAbsent(cluster, new PersistentQueue("replicating_" + cluster, queueRootDir, queueConfig))) {
        //   for (i <- 0 until threadsPerCluster) { 
        //     // threadpool.submit(new Runnable { def run() { process(cluster) } })
        //   }
        // }
        // val queue = queueMap.get(cluster)
        // queue.setup()
        // queue
        error("queue not found")
      }
      case queue => queue
    }
  }

  def start() {

  }

  def shutdown() {
    if (!threadpool.isShutdown) threadpool.shutdown()
  }

  @tailrec
  final def process(cluster: String) {
    val queue = getQueue(cluster)

    if (!queue.isClosed) {
      queue.removeReceive(Some(Time.fromMilliseconds(System.currentTimeMillis + QueuePollTimeout)), true) foreach { item =>
        try {
          jobRelay(cluster)(Iterator(item.data).toIterable)
          queue.confirmRemove(item.xid)
        } catch { case e =>
          // log error
          queue.unremove(item.xid)
        }
      }

      process(cluster)
    }
  }
}