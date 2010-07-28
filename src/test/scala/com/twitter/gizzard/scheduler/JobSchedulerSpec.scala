package com.twitter.gizzard.scheduler

import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.Config
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification


object JobSchedulerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JobScheduler" should {
    val bob = 1
    val mary = 2
    val queue = mock[ErrorHandlingJobQueue]
    val executorScheduler = new JobScheduler("scheduler", 5, queue)

    "retryErrors" in {
      expect {
        one(queue).retry()
      }
      executorScheduler.retryErrors()
    }

    "pause" in {
      expect {
        one(queue).pause()
      }
      executorScheduler.pause()
    }

    "shutdown" in {
      expect {
        one(queue).pause()
        one(queue).shutdown()
      }
      executorScheduler.shutdown()
    }

    "resume" in {
      expect {
        one(queue).resume()
      }
      executorScheduler.resume()
    }
  }
}
