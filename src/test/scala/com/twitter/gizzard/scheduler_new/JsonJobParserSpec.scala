package com.twitter.gizzard.scheduler

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.json.Json

class JsonJobParserSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JsonJobParser" should {
    val attributes = Map("a" -> 1)
    val jobMap = Map("Job" -> attributes)
    val codec = mock[JsonCodec[JsonJob]]
    val job = mock[JsonJob]
    val jobParser = new JsonJobParser[JsonJob] {
      def apply(codec: JsonCodec[JsonJob], json: Map[String, Any]) = {
        job
      }
    }

    "parse" in {
      "simple job" in {
        expect {
          one(job).errorCount_=(0)
          one(job).errorMessage_=(any[String])
        }

        jobParser.parse(codec, attributes) mustEqual job
      }

      "nested job" in {
        expect {
          one(codec).inflate(jobMap) willReturn job
        }

        val nestedAttributes = Map("tasks" -> List(jobMap))
        new JsonNestedJobParser().parse(codec, nestedAttributes) mustEqual new JsonNestedJob(List(job))
      }

      "errors" in {
        expect {
          one(job).errorCount_=(23)
          one(job).errorMessage_=("Good heavens!")
        }

        jobParser.parse(codec, attributes ++ Map("error_count" -> 23, "error_message" -> "Good heavens!")) mustEqual job
      }
    }

    "toJson" in {
      val job = new JsonJob {
        def toMap = attributes
        override def className = "FakeJob"
        def apply() { }
      }

      "JsonJob" in {
        job.toJson mustEqual """{"FakeJob":{"a":1,"error_count":0,"error_message":"(none)"}}"""
      }

      "JsonNestedJob" in {
        val nestedJob = new JsonNestedJob[JsonJob](List(job))

        nestedJob.toJson mustEqual """{"com.twitter.gizzard.scheduler.JsonNestedJob":{"tasks":[{"FakeJob":{"a":1}}],"error_count":0,"error_message":"(none)"}}"""
      }

      "errors" in {
        job.errorCount = 23
        job.errorMessage = "Good heavens!"

        job.toJson mustEqual """{"FakeJob":{"a":1,"error_count":23,"error_message":"Good heavens!"}}"""
      }
    }
  }
}
