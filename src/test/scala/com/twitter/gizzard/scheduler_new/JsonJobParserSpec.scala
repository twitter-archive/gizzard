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
      expect {
        one(job).errorCount_=(0)
        one(job).errorMessage_=(any[String])
      }

      jobParser.parse(codec, jobMap) mustEqual job
    }

/*
    "when a job fails to parse" >> {
      jobParser.parse("gobbledygook") must throwA[UnparsableJobException]
    }
    */
  }
}
