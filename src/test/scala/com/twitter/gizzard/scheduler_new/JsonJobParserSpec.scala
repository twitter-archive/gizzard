package com.twitter.gizzard
package scheduler

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.json.Json

class JsonJobParserSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JsonJobParser" should {
    val attributes = Map("a" -> 1)
    val jobMap = Map("Job" -> attributes)
    val codec = mock[JsonCodec]
    val job = mock[JsonJob]
    val jobParser = new JsonJobParser {
      def apply(json: Map[String, Any]) = {
        job
      }
    }

    "parse" in {
      "simple job" in {
        expect {
          one(job).errorCount_=(0)
          one(job).errorMessage_=(any[String])
        }

        jobParser.parse(attributes) mustEqual job
      }

      "nested job" in {
        expect {
          one(codec).inflate(jobMap) willReturn job
          allowing(job).className willReturn "ugh"
          allowing(job).toMap willReturn Map.empty[String, String]
        }

        val nestedAttributes = Map("tasks" -> List(jobMap))
        new JsonNestedJobParser(codec).parse(nestedAttributes) mustEqual new JsonNestedJob(List(job))
      }

      "errors" in {
        expect {
          one(job).errorCount_=(23)
          one(job).errorMessage_=("Good heavens!")
        }

        jobParser.parse(attributes ++ Map("error_count" -> 23, "error_message" -> "Good heavens!")) mustEqual job
      }
    }

    "toJson" in {
      val job = new JsonJob {
        def toMap = attributes
        override def className = "FakeJob"
        def apply() { }
      }

      "JsonJob" in {
        val json = job.toJson
        json mustMatch "\"FakeJob\""
        json mustMatch "\"a\":1"
        json mustMatch "\"error_count\":0"
        json mustMatch "\"error_message\":\"\\(none\\)\""
      }

      "JsonNestedJob" in {
        val nestedJob = new JsonNestedJob(List(job))
        val json = nestedJob.toJson

        json mustMatch "\"com.twitter.gizzard.scheduler.JsonNestedJob\":\\{"
        json mustMatch "\"error_count\":0"
        json mustMatch "\"error_message\":\"\\(none\\)\""
        json mustMatch "\"tasks\":\\[\\{\"FakeJob\":\\{\"a\":1\\}\\}\\]"
      }

      "errors" in {
        job.errorCount = 23
        job.errorMessage = "Good heavens!"

        val json = job.toJson
        json mustMatch "\\{\"FakeJob\":\\{"
        json mustMatch "\"a\":1"
        json mustMatch "\"error_count\":23"
        json mustMatch "\"error_message\":\"Good heavens!\""
      }
    }
  }
}
