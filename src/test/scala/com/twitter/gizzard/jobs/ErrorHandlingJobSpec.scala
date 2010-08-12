package com.twitter.gizzard.jobs

import scala.collection.mutable
import com.twitter.json.Json
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import shards.ShardRejectedOperationException
import com.twitter.xrayspecs.TimeConversions._
import scheduler.{ErrorHandlingConfig, MessageQueue}


object ErrorHandlingJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "ErrorHandlingJob" should {
    val job = mock[Job]
    val errorQueue = mock[MessageQueue[Schedulable, Job]]
    val errorHandlingConfig = ErrorHandlingConfig(1.minute, 5,
                                                  mock[MessageQueue[String, String]],
                                                  mock[MessageQueue[Schedulable, Job]],
                                                  mock[MessageQueue[String, String]],
                                                  mock[JobParser])
    val errorHandlingParser = new ErrorHandlingJobParser(errorHandlingConfig, errorQueue)
    val errorHandlingJob = new ErrorHandlingJob(job, 0, "", errorQueue, errorHandlingConfig)

    expect {
      allowing(job).className willReturn "foo"
      allowing(job).toMap willReturn Map("a" -> 1)
      allowing(job).toJson willReturn Json.build(Map("Job" -> Map("a" -> 1))).toString
    }

    "register errors" in {
      expect {
        allowing(job).apply() willThrow new Exception("ouch")
        one(errorQueue).put(errorHandlingJob)
      }

      Json.parse(errorHandlingJob.toJson) mustEqual Map("foo" -> Map("a" -> 1, "error_count" -> 0, "error_message" -> ""))
      errorHandlingJob()
      Json.parse(errorHandlingJob.toJson) mustEqual Map("foo" -> Map("a" -> 1, "error_count" -> 1, "error_message" -> "java.lang.Exception: ouch"))
    }

    "when the job errors too much" >> {
      val badJobQueue = errorHandlingConfig.badJobQueue
      expect {
        allowing(job).apply() willThrow new Exception("ouch")
        (errorHandlingConfig.errorLimit).of(errorQueue).put(errorHandlingJob) then
        one(badJobQueue).put(errorHandlingJob)
      }
      (0 until errorHandlingConfig.errorLimit + 1) foreach { _ => errorHandlingJob() }
    }

    "when the shard is darkmoded and the job has errored a lot" >> {
      config("errors.max_errors_per_job") = 0
      expect {
        allowing(job).apply() willThrow new ShardRejectedOperationException("darkmode")
        one(errorQueue).put(errorHandlingJob)
      }
      errorHandlingJob()
    }
  }
}
