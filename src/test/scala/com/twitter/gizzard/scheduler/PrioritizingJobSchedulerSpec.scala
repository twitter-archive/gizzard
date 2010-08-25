package com.twitter.gizzard.scheduler

import com.twitter.util.TimeConversions._
import net.lag.configgy.Config
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import jobs.Schedulable


object PrioritizingJobSchedulerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "PrioritizingJobScheduler" should {
    val low = mock[JobScheduler]
    val medium = mock[JobScheduler]
    val high = mock[JobScheduler]
    val prioritizingScheduler = new PrioritizingJobScheduler(Map(3 -> low, 2 -> medium, 1 -> high))

    "apply" in {
      val schedulable = mock[Schedulable]

      expect { one(low).apply(schedulable) }
      prioritizingScheduler(3, schedulable)
      expect { one(medium).apply(schedulable) }
      prioritizingScheduler(2, schedulable)
      expect { one(high).apply(schedulable) }
      prioritizingScheduler(1, schedulable)
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
