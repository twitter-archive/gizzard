package com.twitter.gizzard.jobs

import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.Config
import net.lag.logging.Logger
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification


object JobSchedulerSpec extends Specification with JMocker with ClassMocker {
  "JobScheduler" should {
    val bob = 1
    val mary = 2

    var jobQueue: MessageQueue = null
    var errorQueue: MessageQueue = null
    var executorScheduler: JobScheduler = null

    doBefore {
      jobQueue = mock[MessageQueue]
      errorQueue = mock[MessageQueue]
      executorScheduler = new JobScheduler("scheduler", 5, 60.seconds, jobQueue, errorQueue)
    }

    "retryErrors" in {
      expect {
        one(errorQueue).writeTo(jobQueue)
      }
      executorScheduler.retryErrors()
    }

    "pauseWork" in {
      expect {
        one(jobQueue).pause()
        one(errorQueue).pause()
      }
      executorScheduler.pauseWork()
    }

    "shutdown" in {
      expect {
        one(jobQueue).pause()
        one(errorQueue).pause()
        one(jobQueue).shutdown()
        one(errorQueue).shutdown()
      }
      executorScheduler.shutdown()
    }

    "resumeWork" in {
      expect {
        one(jobQueue).resume()
        one(errorQueue).resume()
      }
      executorScheduler.resumeWork()
    }

    "create from config" in {
      val schedulerConfig =
        Config.fromMap(Map("job_queue" -> "jobs", "error_queue" -> "errors", "error_limit" -> "50",
                           "threads" -> "10", "replay_interval" -> "60"))
      val queueConfig = Config.fromMap(Map("journal" -> "false"))
      queueConfig.setConfigMap("scheduler", schedulerConfig)
      val parser = mock[JobParser]
      val scheduler = JobScheduler("scheduler", queueConfig, parser, { _ => }, None)

      scheduler.threadCount mustEqual 10
      scheduler.replayInterval mustEqual 60.seconds
      scheduler.jobQueue must haveClass[KestrelMessageQueue]
      val jobQueue = scheduler.jobQueue.asInstanceOf[KestrelMessageQueue]
      scheduler.errorQueue must haveClass[KestrelMessageQueue]
      val errorQueue = scheduler.errorQueue.asInstanceOf[KestrelMessageQueue]
      jobQueue.queue.name mustEqual "jobs"
      errorQueue.queue.name mustEqual "errors"
      jobQueue.jobParser must haveClass[ErrorHandlingJobParser]
      val ehParser = jobQueue.jobParser.asInstanceOf[ErrorHandlingJobParser]
      ehParser.errorQueue mustBe errorQueue
    }
  }

  "PrioritizingJobScheduler" should {
    var low: JobScheduler = null
    var medium: JobScheduler = null
    var high: JobScheduler = null
    var prioritizingScheduler: PrioritizingScheduler = null

    doBefore {
      low = mock[JobScheduler]
      medium = mock[JobScheduler]
      high = mock[JobScheduler]
      prioritizingScheduler = new PrioritizingScheduler(Map(3 -> low, 2 -> medium, 1 -> high))
    }

    "apply" in {
      val job = mock[Schedulable]

      expect { one(low).apply(job) }
      prioritizingScheduler(3, job)
      expect { one(medium).apply(job) }
      prioritizingScheduler(2, job)
      expect { one(high).apply(job) }
      prioritizingScheduler(1, job)
    }

    "retryErrors" in {
      expect {
        one(low).retryErrors()
        one(medium).retryErrors()
        one(high).retryErrors()
      }
      prioritizingScheduler.retryErrors()
    }

    "pauseWork" in {
      expect {
        one(low).pauseWork()
        one(medium).pauseWork()
        one(high).pauseWork()
      }
      prioritizingScheduler.pauseWork()
    }

    "shutdown" in {
      expect {
        one(low).shutdown()
        one(medium).shutdown()
        one(high).shutdown()
      }
      prioritizingScheduler.shutdown()
    }

    "resumeWork" in {
      expect {
        one(low).resumeWork()
        one(medium).resumeWork()
        one(high).resumeWork()
      }
      prioritizingScheduler.resumeWork()
    }
  }
}
