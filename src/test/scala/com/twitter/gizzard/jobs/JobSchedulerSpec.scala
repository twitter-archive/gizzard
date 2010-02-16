package com.twitter.gizzard.jobs

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
      executorScheduler = new JobScheduler(5, jobQueue, errorQueue)
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
