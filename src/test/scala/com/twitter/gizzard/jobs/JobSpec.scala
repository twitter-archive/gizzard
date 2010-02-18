package com.twitter.gizzard.jobs

import scala.collection.mutable
import com.twitter.json.Json
import com.twitter.ostrich.DevNullStats
import net.lag.configgy.Configgy
import net.lag.logging.{Logger, Level, FileFormatter, StringHandler}
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import sharding.ShardRejectedOperationException


object JobSpec extends Specification with JMocker with ClassMocker {
  "ErrorHandlingJob" should {
    var job: Job = null
    var errorHandlingJob: ErrorHandlingJob = null
    var errorQueue: MessageQueue = null
    var badJobs = new mutable.ListBuffer[String]

    doBefore {
      job = mock[Job]
      errorQueue = mock[MessageQueue]
      val config = new ErrorHandlingConfig(10, { badJobs += _ }, None)
      errorHandlingJob = new ErrorHandlingJob(job, errorQueue, config, 0)
      badJobs.clear()

      expect {
        allowing(job).className willReturn "foo"
        allowing(job).toMap willReturn Map("a" -> 1)
        allowing(job).toJson willReturn Json.build(Map("Job" -> Map("a" -> 1))).toString
      }
    }

    "register errors" in {
      expect {
        one(job).apply() willThrow new Exception("ouch")
        one(errorQueue).put(errorHandlingJob)
      }

      Json.parse(errorHandlingJob.toJson) mustEqual Map("foo" -> Map("a" -> 1, "error_count" -> 0))
      errorHandlingJob()
      Json.parse(errorHandlingJob.toJson) mustEqual Map("foo" -> Map("a" -> 1, "error_count" -> 1))
    }

    "when the job errors too much" >> {
      val config = new ErrorHandlingConfig(0, { badJobs += _ }, None)
      errorHandlingJob = new ErrorHandlingJob(job, errorQueue, config, 0)
      expect {
        one(job).apply() willThrow new Exception("ouch")
        never(errorQueue).put(errorHandlingJob)
      }
      errorHandlingJob()
      badJobs.size mustEqual 1
      badJobs(0) must include("foo")
    }

    "when the shard is darkmoded and the job has errored a lot" >> {
      Configgy.config("errors.max_errors_per_job") = 0
      expect {
        one(job).apply() willThrow new ShardRejectedOperationException("darkmode")
        one(errorQueue).put(errorHandlingJob)
      }
      errorHandlingJob()
      logger.toString mustNot include("foo")
    }

    "ErrorHandlingJobParser" in {
      "register errors" >> {
        val jobParser = mock[JobParser]
        val config = new ErrorHandlingConfig(100, { _ => }, None)
        val errorHandlingParser = new ErrorHandlingJobParser(jobParser, config)
        errorHandlingParser.errorQueue = errorQueue

        expect {
          allowing(jobParser).apply(a[Map[String, Map[String, AnyVal]]]) willReturn job
          allowing(job).apply() willThrow new Exception("ouch")
        }

        errorHandlingJob = errorHandlingParser.apply(job.toJson).asInstanceOf[ErrorHandlingJob]
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

  "BoundJob" should {
    "use the job's original class" in {
      val job = new FakeJob(Map("a" -> 1))
      val boundJob = BoundJob(job, 1973)
      Json.parse(boundJob.toJson) mustEqual Map(job.className -> Map("a" -> 1))
    }
  }

  "JobWithTasks" should {
    "to and from json" >> {
      val jobs = List(mock[Job], mock[Job])
      for (job <- jobs) {
        expect {
          allowing(job).className willReturn "Task"
          allowing(job).toMap willReturn Map("a" -> 1)
        }
      }
      val jobWithTasks = new JobWithTasks(jobs)
      val json = jobWithTasks.toJson
      json mustMatch "JobWithTasks"
      json mustMatch "\"tasks\":"
      json mustMatch "\\{\"Task\":\\{\"a\":1\\}\\}"
    }

    "loggingName" >> {
      val job1 = mock[Job]
      val job2 = mock[Job]
      val jobWithTasks = new JobWithTasks(List(job1, job2))
      val errorQueue = mock[MessageQueue]
      val config = new ErrorHandlingConfig(100, { _ => }, None)
      val errorHandlingJob = new ErrorHandlingJob(jobWithTasks, errorQueue, config, 0)
      expect {
        allowing(job1).loggingName willReturn "Job1"
        allowing(job2).className willReturn "Job2"
        allowing(job2).loggingName willReturn "Job2"
      }
      errorHandlingJob.loggingName mustEqual "Job1,Job2"
    }
  }

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
