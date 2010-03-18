package com.twitter.gizzard.jobs

import scala.collection.mutable
import com.twitter.json.Json
import net.lag.configgy.Configgy
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import shards.ShardRejectedOperationException


object JobWithTasksSpec extends Specification with JMocker with ClassMocker {
  "JobWithTasksParser" should {
    "apply" in {
      val job = mock[Job]
      val jobParser = mock[JobParser]
      val taskJson = Map("Bar" -> Map("a" -> 1))
      val jobWithTasksParser = new JobWithTasksParser(jobParser)
      expect {
        one(jobParser).apply(taskJson) willReturn job
      }
      val result = jobWithTasksParser(Map("com.twitter.gizzard.jobs.JobWithTasks" -> Map("tasks" ->
        List(taskJson)
      )))
      result mustEqual new JobWithTasks(List(job))
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
      expect {
        allowing(job1).loggingName willReturn "Job1"
        allowing(job2).className willReturn "Job2"
        allowing(job2).loggingName willReturn "Job2"
      }
      jobWithTasks.loggingName mustEqual "Job1,Job2"
    }
  }
}
