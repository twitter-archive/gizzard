package com.twitter.gizzard.scheduler_new

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.json.Json

class JsonJobParserSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JsonJobParser" should {
    val attributes = Map("a" -> 1)
    val jobMap = Map("Job" -> attributes)
    val codec = mock[JsonCodec[String, JsonJob[String]]]
    val job = mock[JsonJob[String]]
    val jobParser = new JsonJobParser[String, JsonJob[String]] {
      val environment = "Environment"
      def apply(codec: JsonCodec[String, JsonJob[String]], json: Map[String, Any]) = {
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
        jobParser.parse(codec, nestedAttributes) mustEqual new JsonNestedJob(jobParser.environment, List(job))
      }
    }

    "toJson" in {
      val job = new JsonJob[String] {
        val environment = "Environment"
        def toMap = attributes
        override def className = "FakeJob"
        def apply(environment: String) { }
      }

      "JsonJob" in {
        job.toJson mustEqual """{"FakeJob":{"a":1,"error_count":0,"error_message":"(none)"}}"""
      }

      "JsonNestedJob" in {
        val nestedJob = new JsonNestedJob[String, JsonJob[String]]("Environment", List(job))

        nestedJob.toJson mustEqual """{"com.twitter.gizzard.scheduler_new.JsonNestedJob":{"tasks":[{"FakeJob":{"a":1}}],"error_count":0,"error_message":"(none)"}}"""
      }
    }
  }
}
