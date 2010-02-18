package com.twitter.gizzard.thrift

import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import com.twitter.gizzard.thrift.conversions.Sequences._


object JobManagerServiceSpec extends Specification with JMocker with ClassMocker {
  val scheduler = mock[jobs.PrioritizingScheduler]
  val subScheduler = mock[jobs.JobScheduler]
  val service = new JobManagerService(scheduler)

  "JobManagerService" should {
    "retry_errors" in {
      expect {
        one(scheduler).retryErrors()
      }

      service.retry_errors()
    }

    "stop_writes" in {
      expect {
        one(scheduler).pauseWork()
      }

      service.stop_writes()
    }

    "resume_writes" in {
      expect {
        one(scheduler).resumeWork()
      }

      service.resume_writes()
    }

    "retry_errors_for" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).retryErrors()
      }

      service.retry_errors_for(3)
    }

    "stop_writes_for" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).pauseWork()
      }

      service.stop_writes_for(3)
    }

    "resume_writes_for" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).resumeWork()
      }

      service.resume_writes_for(3)
    }

    "is_writing" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).isShutdown willReturn false
      }

      service.is_writing(3) mustEqual true
    }

    "inject_job" in {
      val job = capturingParam[jobs.Schedulable]

      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).apply(job.capture)
      }

      service.inject_job(3, "maxwell integer")
      job.captured.toJson mustEqual "maxwell integer"
    }
  }
}
