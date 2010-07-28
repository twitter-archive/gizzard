package com.twitter.gizzard.jobs

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.json.Json


object BoundJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "BoundJobParser" should {
    "apply" in {
      val job = new fake.Job(Map("a" -> 1, "error_count" -> 0))

      (new BoundJobParser(fake.UnboundJobParser, 1)).apply(job.toJson) mustEqual new BoundJob(job, 1)
    }
  }

  "BoundJob" should {
    "use the job's original class" in {
      val job = new fake.Job(Map("a" -> 1))
      val boundJob = new BoundJob(job, 1973)
      Json.parse(boundJob.toJson) mustEqual Map(job.className -> Map("a" -> 1))
    }
  }
}
