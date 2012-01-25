package com.twitter.gizzard.scheduler

import scala.annotation.tailrec
import com.twitter.util.Time
import com.twitter.gizzard.nameserver.JobRelay
import net.lag.kestrel.config.QueueConfig
import java.util.concurrent.ConcurrentHashMap

class JobAsyncReplicator(jobRelay: => JobRelay, queueConfig: QueueConfig, queueRootDir: String) {
  
  private val QueuePollTimeout = 1000 // 1 second
  
  val queueMap = new ConcurrentHashMap[String, PersistentQueue]
  val threadPool 
  
  def enqueue(job: Array[Byte]) {
    jobRelay.clusters.foreach { getQueue(_).add(job) }
  }
  
  def getQueue(cluster: String) = {
    queueMap.get(cluster) match {
      case null => { 
        queueMap.putIfAbsent(cluster, new PersistentQueue("replicating_" + cluster, queueRootDir, queueConfig))
        queueMap.get(cluster)
      }
      case queue => queue
    }
  }
  
  @tailrec
  def process(cluster: String) {
    val queue = getQueue(cluster)
    
    if (!queue.isClosed) {
      queue.removeReceive(Some(Time.fromMilliseconds(System.currentTimeMillis + QueuePollTimeout)), true) foreach { item =>
        try {
          jobRelay(cluster)(item.data)
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