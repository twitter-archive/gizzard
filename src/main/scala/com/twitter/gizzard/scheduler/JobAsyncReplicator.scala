package com.twitter.gizzard.scheduler

import com.twitter.gizzard.nameserver.JobRelay
import net.lag.kestrel.config.QueueConfig

class JobAsyncReplicator(jobRelay: => JobRelay, queueConfig: QueueConfig, queueRootDir: String) {
 
  def enqueue(job: Array[Byte]) {
  }  
}