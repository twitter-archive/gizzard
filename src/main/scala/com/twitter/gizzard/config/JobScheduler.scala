package com.twitter.gizzard.config

import net.lag.kestrel.config.PersistentQueue


trait Scheduler {
  val jobQueueName: String
  val errorQueueName: String
  val queue: PersistentQueue
}

trait JobScheduler {
  val path: String
  

}
