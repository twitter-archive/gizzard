package com.twitter.gizzard.scheduler

import scala.collection.mutable
import com.twitter.json.Json
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification

class JournaledJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JournaledJob" should {
    val environment = "environment"
    val job = mock[JsonJob[String]]
    val queue = mock[String => Unit]

    "journal on success" in {
      expect {
        one(job).environment willReturn environment
        one(job).apply(environment)
        one(job).toJson willReturn "hello"
        one(queue).apply("hello")
      }

      val journaledJob = new JournaledJob[String](job, queue)
      journaledJob.apply(environment)
    }

    "not journal on failure" in {
      expect {
        one(job).environment willReturn environment
        one(job).apply(environment) willThrow(new Exception("aiee"))
      }

      val journaledJob = new JournaledJob[String](job, queue)
      journaledJob.apply(environment) must throwA[Exception]
    }
  }
}
