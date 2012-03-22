package com.twitter.gizzard.scheduler

import java.util.concurrent.atomic.AtomicInteger
import com.twitter.conversions.time._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.gizzard.shards._
import com.twitter.gizzard.ConfiguredSpecification
import com.twitter.gizzard.nameserver.JobRelay
import com.twitter.gizzard.Stats


class JobSchedulerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JobScheduler" should {
    val asyncReplicator = mock[JobAsyncReplicator]
    val queue = mock[JobQueue]
    val errorQueue = mock[JobQueue]
    val badJobQueue = mock[JobQueue]
    val jobRelatedQueues = List(queue, errorQueue, badJobQueue)
    val job1 = mock[JsonJob]
    val ticket1 = mock[Ticket]
    val codec = mock[JsonCodec]
    val shardId = ShardId("fake", "shard")

    var jobScheduler: JobScheduler = null
    var replicatingJobScheduler: JobScheduler = null
    val liveThreads = new AtomicInteger(0)

    val jsonBytes = "jsonBytes".getBytes("UTF-8")

    val MAX_ERRORS = 100
    val MAX_FLUSH = 10
    val JITTER_RATE = 0.01f


    doBefore {
      jobScheduler = new JobScheduler("test", 1, 1.minute, MAX_ERRORS, MAX_FLUSH, JITTER_RATE, false, asyncReplicator,
                                      queue, errorQueue, badJobQueue) {
      replicatingJobScheduler =
        new JobScheduler("test", 1, 1.minute, MAX_ERRORS, MAX_FLUSH, JITTER_RATE, true, asyncReplicator,
                         queue, errorQueue, badJobQueue)
        override def processWork() {
          liveThreads.incrementAndGet()
          try {
            Thread.sleep(60.minutes.inMillis)
          } finally {
            liveThreads.decrementAndGet()
          }
        }
      }
    }

    "start & shutdown" in {
      expect {
        jobRelatedQueues.foreach(one(_).start())
        one(queue).isShutdown willReturn false
      }

      jobScheduler.running mustEqual false
      jobScheduler.workerThreads.size mustEqual 0
      jobScheduler.start()
      jobScheduler.running mustEqual true
      jobScheduler.workerThreads.size mustEqual 1
      liveThreads.get() mustEqual 1
      jobScheduler.isShutdown mustEqual false

      expect {
        jobRelatedQueues.foreach(one(_).shutdown())
        one(queue).isShutdown willReturn true
      }

      jobScheduler.shutdown()
      jobScheduler.running mustEqual false
      jobScheduler.workerThreads.size mustEqual 0
      liveThreads.get() mustEqual 0
      jobScheduler.isShutdown mustEqual true
    }

    "pause & resume" in {
      expect {
        jobRelatedQueues.foreach(one(_).start())
      }

      jobScheduler.start()

      expect {
        jobRelatedQueues.foreach(one(_).pause())
      }

      jobScheduler.pause()
      jobScheduler.running mustEqual true
      jobScheduler.workerThreads.size mustEqual 0
      liveThreads.get() mustEqual 0

      expect {
        jobRelatedQueues.foreach(one(_).resume())
      }

      jobScheduler.resume()
      jobScheduler.running mustEqual true
      jobScheduler.workerThreads.size mustEqual 1
      liveThreads.get() mustEqual 1

      expect {
        jobRelatedQueues.foreach(one(_).shutdown())
      }

      jobScheduler.shutdown()
      jobScheduler.running mustEqual false
      jobScheduler.workerThreads.size mustEqual 0
      liveThreads.get() mustEqual 0
    }

    "retryErrors" in {
      expect {
        one(errorQueue).size  willReturn 2
        one(errorQueue).get() willReturn Some(ticket1)
        one(ticket1).job      willReturn job1
        one(queue).put(job1)
        one(ticket1).ack()
        one(errorQueue).get() willReturn None
      }

      jobScheduler.retryErrors()
    }

    "checkExpiredJobs" in {
      expect {
        one(errorQueue).checkExpiration(10)
      }

      jobScheduler.checkExpiredJobs()
    }

    "put" in {
      expect {
        one(queue).put(job1)
      }

      jobScheduler.put(job1)
    }

    "process" in {
      "success" in {
        expect {
          one(queue).get() willReturn Some(ticket1)
          one(ticket1).job willReturn job1
          one(job1).apply()
          one(job1).nextJob willReturn None
          one(ticket1).ack()
        }

        jobScheduler.process()
      }

      "blocked" in {
        expect {
          one(queue).get() willReturn Some(ticket1)
          one(ticket1).job willReturn job1
          one(job1).apply() willThrow new ShardOfflineException(shardId)
          one(ticket1).ack()
          one(job1).nextJob willReturn None
          one(errorQueue).put(job1)
        }

        jobScheduler.process()
      }

      "error" in {
        expect {
          one(queue).get() willReturn Some(ticket1)
          one(ticket1).job willReturn job1
          one(job1).apply() willThrow new Exception("aie!")
          one(job1).errorCount willReturn 0
          one(job1).errorCount_=(1)
          one(job1).errorMessage_=("java.lang.Exception: aie!")
          one(job1).errorCount willReturn 1
          one(ticket1).ack()
          one(job1).nextJob willReturn None
          one(errorQueue).put(job1)
        }

        jobScheduler.process()
      }

      "handle queue errors" in {
        Stats.clearAll()
        expect {
          one(queue).get() willThrow new Exception("bad queue")
        }

        jobScheduler.process()
        Stats.getCounter("uncaught-exceptions")() mustEqual 1
      }

      "too many errors" in {
        expect {
          one(queue).get() willReturn Some(ticket1)
          one(ticket1).job willReturn job1
          one(job1).apply() willThrow new Exception("aie!")
          one(job1).errorCount willReturn MAX_ERRORS
          one(job1).errorCount_=(MAX_ERRORS + 1)
          one(job1).errorMessage_=("java.lang.Exception: aie!")
          one(job1).errorCount willReturn MAX_ERRORS + 1
          one(ticket1).ack()
          one(job1).nextJob willReturn None
          one(badJobQueue).put(job1)
        }

        jobScheduler.process()
      }

      "replicating" in {
        expect {
          one(queue).get() willReturn Some(ticket1)
          one(ticket1).job willReturn job1
          one(job1).shouldReplicate willReturn true
          one(job1).wasReplicated willReturn false
          one(job1).toJsonBytes willReturn jsonBytes
          one(asyncReplicator).enqueue(jsonBytes)
          one(job1).setReplicated
          one(job1).apply()
          one(job1).nextJob willReturn None
          one(ticket1).ack()
        }

        replicatingJobScheduler.process()
      }

      "already replicated" in {
        expect {
          one(queue).get() willReturn Some(ticket1)
          one(ticket1).job willReturn job1
          one(job1).shouldReplicate willReturn true
          one(job1).wasReplicated willReturn true
          one(job1).apply()
          one(job1).nextJob willReturn None
          one(ticket1).ack()
        }

        replicatingJobScheduler.process()
      }

    }
  }
}
