package com.twitter.gizzard.scheduler

import scala.collection.mutable
import com.twitter.json.Json
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification

trait FakeJob extends JsonJob {
  type Environment = String
}

class JournaledJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JournaledJob" should {
    val environment = "environment"
    val job = mock[FakeJob]
    val queue = mock[String => Unit]

    "journal on success" in {
      expect {
        one(job).apply()
        one(job).toJson willReturn "hello"
        one(queue).apply("hello")
      }

      val journaledJob = new JournaledJob(job, queue)
      journaledJob.apply()
    }

    "not journal on failure" in {
      expect {
        one(job).apply() willThrow(new Exception("aiee"))
      }

      val journaledJob = new JournaledJob(job, queue)
      journaledJob.apply() must throwA[Exception]
    }
  }
}
