package com.twitter.gizzard.jobs

import scala.collection.mutable
import com.twitter.json.Json
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import shards.ShardRejectedOperationException
import com.twitter.xrayspecs.TimeConversions._
import scheduler.{ErrorHandlingConfig, MessageQueue}
import com.twitter.ostrich.DevNullStats


object ErrorHandlingJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val job = mock[Job]
  val errorQueue = mock[MessageQueue[Schedulable, Job]]
  val errorHandlingConfig = ErrorHandlingConfig(1.minute, 5,
                                                mock[MessageQueue[String, String]],
                                                mock[MessageQueue[Schedulable, Job]],
                                                mock[MessageQueue[String, String]],
                                                mock[JobParser])
  val errorHandlingJob = new ErrorHandlingJob(job, 0, errorQueue, errorHandlingConfig)
  val errorHandlingParser = new ErrorHandlingJobParser(errorHandlingConfig, errorQueue)

  "ErrorHandlingJob" should {
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

      Json.parse(errorHandlingJob.toJson) mustEqual Map("foo" -> Map("a" -> 1, "error_count" -> 0))
      errorHandlingJob()
      Json.parse(errorHandlingJob.toJson) mustEqual Map("foo" -> Map("a" -> 1, "error_count" -> 1))
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

  "ErrorHandlingJobParser" should {
    "register and increment errors" >> {
      val attributes = Map("a" -> 1)
      expect {
        allowing(errorHandlingConfig.jobParser).apply(a[Map[String, Map[String, AnyVal]]]) willReturn job
        allowing(job).className willReturn "className"
        allowing(job).apply() willThrow new Exception("ouch")
        allowing(job).toMap willReturn attributes
      }

      val errorHandlingJob = errorHandlingParser("{\"className\":{\"a\":1}}").asInstanceOf[ErrorHandlingJob]
      errorHandlingJob.errorCount mustEqual 0
      expect {
        one(errorQueue).put(errorHandlingJob)
      }
      errorHandlingJob()
      errorHandlingParser.apply(errorHandlingJob.toJson)
      errorHandlingJob.errorCount mustEqual 1
      expect {
        one(errorQueue).put(errorHandlingJob)
      }
      errorHandlingJob()
      errorHandlingParser.apply(errorHandlingJob.toJson)
      errorHandlingJob.errorCount mustEqual 2
    }
  }

}
