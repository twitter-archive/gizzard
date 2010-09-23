package com.twitter.gizzard.scheduler_new

import scala.collection.mutable
import com.twitter.json.Json
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import shards.ShardRejectedOperationException

class NestedJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "NestedJob" should {
    val job1 = mock[Job[String]]
    val job2 = mock[Job[String]]
    val job3 = mock[Job[String]]
    val nestedJob = new NestedJob("environment", List(job1, job2, job3))

    "loggingName" in {
      expect {
        one(job1).loggingName willReturn "job1"
        one(job2).loggingName willReturn "job2"
        one(job3).loggingName willReturn "job3"
      }

      nestedJob.loggingName mustEqual "job1,job2,job3"
    }

    "equals" in {
      nestedJob mustEqual new NestedJob("foo", List(job1, job2, job3))
    }

    "apply" in {
      "success" in {
        expect {
          one(job1).apply("environment")
          one(job2).apply("environment")
          one(job3).apply("environment")
        }

        nestedJob.apply()
        nestedJob.taskQueue.size mustEqual 0
      }

      "instant failure" in {
        expect {
          one(job1).apply("environment") willThrow new Exception("oops!")
        }

        nestedJob.apply() must throwA[Exception]
        nestedJob.taskQueue.size mustEqual 3
      }

      "eventual failure" in {
        expect {
          one(job1).apply("environment")
          one(job2).apply("environment")
          one(job3).apply("environment") willThrow new Exception("oops!")
        }

        nestedJob.apply() must throwA[Exception]
        nestedJob.taskQueue.size mustEqual 1
      }
    }
  }
}
