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
}
