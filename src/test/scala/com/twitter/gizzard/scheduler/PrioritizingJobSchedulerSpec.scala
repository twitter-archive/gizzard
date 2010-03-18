package com.twitter.gizzard.scheduler

import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.Config
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import jobs.Job


object PrioritizingJobSchedulerSpec extends Specification with JMocker with ClassMocker {
  "PrioritizingJobScheduler" should {
    val low = mock[JobScheduler]
    val medium = mock[JobScheduler]
    val high = mock[JobScheduler]
    val prioritizingScheduler = new PrioritizingJobScheduler(Map(3 -> low, 2 -> medium, 1 -> high))

    "apply" in {
      val job = mock[Job]

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

    "pause" in {
      expect {
        one(low).pause()
        one(medium).pause()
        one(high).pause()
      }
      prioritizingScheduler.pause()
    }

    "shutdown" in {
      expect {
        one(low).shutdown()
        one(medium).shutdown()
        one(high).shutdown()
      }
      prioritizingScheduler.shutdown()
    }

    "resume" in {
      expect {
        one(low).resume()
        one(medium).resume()
        one(high).resume()
      }
      prioritizingScheduler.resume()
    }
  }
}
