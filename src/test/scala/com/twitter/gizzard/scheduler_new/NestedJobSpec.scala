package com.twitter.gizzard.scheduler

import scala.collection.mutable
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import com.twitter.gizzard.ConfiguredSpecification


class NestedJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "NestedJob" should {
    val job1 = mock[JsonJob]
    val job2 = mock[JsonJob]
    val job3 = mock[JsonJob]
    val nestedJob = new JsonNestedJob(List(job1, job2, job3))

    "loggingName" in {
      expect {
        one(job1).loggingName willReturn "job1"
        one(job2).loggingName willReturn "job2"
        one(job3).loggingName willReturn "job3"
      }

      nestedJob.loggingName mustEqual "job1,job2,job3"
    }

    "equals" in {
      expect {
        atLeast(1).of(job1).className willReturn "JsonJob"
        atLeast(1).of(job1).toMap willReturn Map[String, String]()
        atLeast(1).of(job2).className willReturn "JsonJob"
        atLeast(1).of(job2).toMap willReturn Map[String, String]()
        atLeast(1).of(job3).className willReturn "JsonJob"
        atLeast(1).of(job3).toMap willReturn Map[String, String]()
      }

      nestedJob mustEqual new JsonNestedJob(List(job1, job2, job3))
    }

    "apply" in {
      "success" in {
        expect {
          one(job1).apply()
          one(job2).apply()
          one(job3).apply()
        }

        nestedJob.apply()
        nestedJob.taskQueue.size mustEqual 0
      }

      "instant failure" in {
        expect {
          one(job1).apply() willThrow new Exception("oops!")
        }

        nestedJob.apply() must throwA[Exception]
        nestedJob.taskQueue.size mustEqual 3
      }

      "eventual failure" in {
        expect {
          one(job1).apply()
          one(job2).apply()
          one(job3).apply() willThrow new Exception("oops!")
        }

        nestedJob.apply() must throwA[Exception]
        nestedJob.taskQueue.size mustEqual 1
      }
    }
  }
}
