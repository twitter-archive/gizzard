package com.twitter.gizzard.jobs

import scala.collection.mutable
import com.twitter.json.Json
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import shards.ShardRejectedOperationException


object JournaledJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JournaledJob" should {
    "journal on success" in {
      val job = mock[Job]
      val queue = mock[String => Unit]
      expect {
        one(job).apply()
        one(job).toJson willReturn "hello"
        one(queue).apply("hello")
      }
      new JournaledJob(job, queue).apply()
    }

    "not journal on failure" in {
      val job = mock[Job]
      val queue = mock[String => Unit]
      expect {
        one(job).apply() willThrow(new Exception("aiee"))
      }
      new JournaledJob(job, queue).apply() must throwA[Exception]
    }
  }
}
