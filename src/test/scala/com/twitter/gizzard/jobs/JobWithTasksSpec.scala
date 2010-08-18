package com.twitter.gizzard.jobs

import scala.collection.mutable
import com.twitter.json.Json
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import shards.ShardRejectedOperationException


object JobWithTasksSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JobWithTasksParser" should {
    "apply" in {
      val job = mock[Job]
      val jobParser = mock[JobParser]
      val taskJson = Map("Bar" -> Map("a" -> 1))
      val jobWithTasksParser = new JobWithTasksParser(jobParser)
      expect {
        one(jobParser).apply(taskJson) willReturn job
      }
      val result = jobWithTasksParser(Map("theClassNameIsIgnored" -> Map("tasks" ->
        List(taskJson)
      )))
      result mustEqual new JobWithTasks(List(job))
    }

    "discard tasks as it goes" in {
      val a = mock[Job]
      expect { one(a).apply }

      val b = new Job {
        def toMap = Map.empty
        var apply = {}
      }

      val jobs = new JobWithTasks(List(a, b))

      b.apply = (() => { jobs.remainingTasks.length mustEqual 1 })

      jobs.apply()
    }

    "discard only after success" in {
      val a = mock[Job]
      expect { one(a).apply }

      val b = new Job {
        def toMap = Map.empty
        var apply = {}
      }

      val jobs = new JobWithTasks(List(a, b))

      b.apply = (() => { throw new RuntimeException("boom") })

      try {
        jobs.apply()
      } catch {
        case e: RuntimeException =>
          jobs.remainingTasks.length mustEqual 1
      }
    }
  }
}
