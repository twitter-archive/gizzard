package com.twitter.gizzard.scheduler

import java.util.concurrent.atomic.AtomicInteger
import com.twitter.xrayspecs.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import shards.ShardRejectedOperationException

class JobSchedulerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JobScheduler" should {
    val queue = mock[JobQueue[Job[String]]]
    val errorQueue = mock[JobQueue[Job[String]]]
    val badJobQueue = mock[JobConsumer[Job[String]]]
    val job1 = mock[Job[String]]
    val ticket1 = mock[Ticket[Job[String]]]

    var jobScheduler: JobScheduler[Job[String]] = null
    val liveThreads = new AtomicInteger(0)

    val MAX_ERRORS = 100

    doBefore {
      jobScheduler = new JobScheduler("test", 1, 1.minute, MAX_ERRORS, queue, errorQueue, badJobQueue) {
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
        one(queue).start()
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
        one(queue).pause()
        one(queue).shutdown()
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
        one(queue).start()
      }

      jobScheduler.start()

      expect {
        one(queue).pause()
      }

      jobScheduler.pause()
      jobScheduler.running mustEqual true
      jobScheduler.workerThreads.size mustEqual 0
      liveThreads.get() mustEqual 0

      expect {
        one(queue).resume()
      }

      jobScheduler.resume()
      jobScheduler.running mustEqual true
      jobScheduler.workerThreads.size mustEqual 1
      liveThreads.get() mustEqual 1

      expect {
        one(queue).pause()
        one(queue).shutdown()
      }

      jobScheduler.shutdown()
      jobScheduler.running mustEqual false
      jobScheduler.workerThreads.size mustEqual 0
      liveThreads.get() mustEqual 0
    }

    "retryErrors" in {
      expect {
        one(errorQueue).drainTo(queue)
      }

      jobScheduler.retryErrors()
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
          one(ticket1).ack()
        }

        jobScheduler.process()
      }

      "darkmode" in {
        expect {
          one(queue).get() willReturn Some(ticket1)
          one(ticket1).job willReturn job1
          one(job1).apply() willThrow new ShardRejectedOperationException("darkmoded!")
          one(ticket1).ack()
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
          one(errorQueue).put(job1)
        }

        jobScheduler.process()
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
          one(badJobQueue).put(job1)
        }

        jobScheduler.process()
      }
    }
  }
}
