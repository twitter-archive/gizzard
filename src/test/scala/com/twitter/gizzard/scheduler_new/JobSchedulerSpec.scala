package com.twitter.gizzard.scheduler

import java.util.concurrent.atomic.AtomicInteger
import com.twitter.xrayspecs.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import net.lag.configgy.Config
import shards.{ShardId, ShardRejectedOperationException}

class JobSchedulerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JobScheduler" should {
    val queue = mock[JobQueue[Job]]
    val errorQueue = mock[JobQueue[Job]]
    val badJobQueue = mock[JobConsumer[Job]]
    val job1 = mock[Job]
    val ticket1 = mock[Ticket[Job]]
    val codec = mock[Codec[Job]]
    val shardId = ShardId("fake", "shard")

    var jobScheduler: JobScheduler[Job] = null
    val liveThreads = new AtomicInteger(0)

    val MAX_ERRORS = 100
    val MAX_FLUSH = 10
    val JITTER_RATE = 0.01f

    doBefore {
      jobScheduler = new JobScheduler("test", 1, 1.minute, MAX_ERRORS, MAX_FLUSH, JITTER_RATE,
                                      queue, errorQueue, Some(badJobQueue)) {
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

    "configure" in {
      "kestrel queue" in {
        val config = Config.fromMap(Map("path" -> "/tmp", "write.job_queue" -> "write1",
                                        "write.error_queue" -> "error1", "write.threads" -> "100",
                                        "write.jitter_rate" -> "0.05",
                                        "write.error_delay" -> "60",
                                        "write.flush_limit" -> "130",
                                        "write.strobe_interval" -> "1000", "write.error_limit" -> "5"))
        val scheduler = JobScheduler("write", config, codec, Some(badJobQueue))
        scheduler.name mustEqual "write"
        scheduler.threadCount mustEqual 100
        scheduler.strobeInterval mustEqual 1.seconds
        scheduler.errorLimit mustEqual 5
        scheduler.flushLimit mustEqual 130
        scheduler.jitterRate mustEqual 0.05f
        scheduler.queue.asInstanceOf[KestrelJobQueue[_]].name mustEqual "write1"
        scheduler.errorQueue.asInstanceOf[KestrelJobQueue[_]].name mustEqual "error1"
        scheduler.badJobQueue mustEqual Some(badJobQueue)
      }

      "memory queue" in {
        val config = Config.fromMap(Map("write.type" -> "memory", "write.job_queue" -> "write1",
                                        "write.error_queue" -> "error1", "write.threads" -> "100",
                                        "write.jitter_rate" -> "0.05",
                                        "write.error_delay" -> "60",
                                        "write.flush_limit" -> "130",
                                        "write.strobe_interval" -> "1000", "write.error_limit" -> "5"))
        val scheduler = JobScheduler("write", config, codec, Some(badJobQueue))
        scheduler.name mustEqual "write"
        scheduler.threadCount mustEqual 100
        scheduler.strobeInterval mustEqual 1.seconds
        scheduler.errorLimit mustEqual 5
        scheduler.flushLimit mustEqual 130
        scheduler.jitterRate mustEqual 0.05f
        scheduler.queue.asInstanceOf[MemoryJobQueue[_]].name mustEqual "write1"
        scheduler.errorQueue.asInstanceOf[MemoryJobQueue[_]].name mustEqual "error1"
        scheduler.badJobQueue mustEqual Some(badJobQueue)
      }
    }

    "start & shutdown" in {
      expect {
        one(queue).start()
        one(errorQueue).start()
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
        one(queue).shutdown()
        one(errorQueue).shutdown()
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
        one(errorQueue).start()
      }

      jobScheduler.start()

      expect {
        one(queue).pause()
        one(errorQueue).pause()
      }

      jobScheduler.pause()
      jobScheduler.running mustEqual true
      jobScheduler.workerThreads.size mustEqual 0
      liveThreads.get() mustEqual 0

      expect {
        one(queue).resume()
        one(errorQueue).resume()
      }

      jobScheduler.resume()
      jobScheduler.running mustEqual true
      jobScheduler.workerThreads.size mustEqual 1
      liveThreads.get() mustEqual 1

      expect {
        one(queue).shutdown()
        one(errorQueue).shutdown()
      }

      jobScheduler.shutdown()
      jobScheduler.running mustEqual false
      jobScheduler.workerThreads.size mustEqual 0
      liveThreads.get() mustEqual 0
    }

    "retryErrors" in {
      expect {
        one(errorQueue).checkExpiration(10)
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
          one(job1).apply() willThrow new ShardRejectedOperationException("darkmoded!", shardId)
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

      "decoder error" in {
        expect {
          one(queue).get() willThrow new UnparsableJsonException("Unparsable json", null)
        }

        jobScheduler.process()
      }
    }
  }
}
