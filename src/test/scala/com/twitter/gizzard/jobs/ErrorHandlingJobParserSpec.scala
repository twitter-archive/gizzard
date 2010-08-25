package com.twitter.gizzard.jobs

import scala.collection.mutable
import com.twitter.json.Json
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import shards.ShardRejectedOperationException
import com.twitter.util.TimeConversions._
import scheduler.{ErrorHandlingConfig, MessageQueue}


object ErrorHandlingJobParserSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "ErrorHandlingJobParser" should {
    "register and increment errors" in {
      val job = mock[Job]
      val errorQueue = mock[MessageQueue[Schedulable, Job]]
      val errorHandlingConfig = ErrorHandlingConfig(1.minute, 5,
                                                    mock[MessageQueue[String, String]],
                                                    mock[MessageQueue[Schedulable, Job]],
                                                    mock[MessageQueue[String, String]],
                                                    mock[JobParser])
      val errorHandlingParser = new ErrorHandlingJobParser(errorHandlingConfig, errorQueue)

      val attributes = Map("a" -> 1)

      expect {
        allowing(errorHandlingConfig.jobParser).apply(a[Map[String, Map[String, AnyVal]]]) willReturn job
        allowing(job).className willReturn "className"
        allowing(job).toMap willReturn attributes
      }

      val errorHandlingJob = errorHandlingParser("{\"className\":{\"a\":1}}").asInstanceOf[ErrorHandlingJob]
      errorHandlingJob.errorCount mustEqual 0

      expect {
        one(job).apply() willThrow new Exception("ouch")
        one(errorQueue).put(errorHandlingJob)
      }

      errorHandlingJob()

      errorHandlingParser.apply(errorHandlingJob.toJson)
      errorHandlingJob.errorCount mustEqual 1
      errorHandlingJob.errorMessage mustEqual "java.lang.Exception: ouch"

      expect {
        one(job).apply() willThrow new Exception("ouch")
        one(errorQueue).put(errorHandlingJob)
      }

      errorHandlingJob()
      errorHandlingParser.apply(errorHandlingJob.toJson)
      errorHandlingJob.errorCount mustEqual 2
      errorHandlingJob.errorMessage mustEqual "java.lang.Exception: ouch"
    }
  }
}
