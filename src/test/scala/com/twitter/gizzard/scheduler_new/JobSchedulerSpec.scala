package com.twitter.gizzard.scheduler_new

import java.util.concurrent.atomic.AtomicInteger
import com.twitter.xrayspecs.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}

class JobSchedulerSpec extends ConfiguredSpecification with JMocker {
  "JobScheduler" should {
    val queue = mock[JobQueue[String, Job[String]]]
    val errorQueue = mock[JobQueue[String, Job[String]]]
    val badJobQueue = mock[JobConsumer[String, Job[String]]]
    var jobScheduler: JobScheduler[String, Job[String]] = null
    val liveThreads = new AtomicInteger(0)

    doBefore {
      jobScheduler = new JobScheduler("test", 1, 1.minute, 100, queue, errorQueue, badJobQueue) {
        override def process() {
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
      }

      jobScheduler.running mustEqual false
      jobScheduler.workerThreads.size mustEqual 0
      jobScheduler.start()
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
  }
}
