package com.twitter.gizzard.jobs

import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import com.twitter.json.Json


object JobParserSpec extends Specification with JMocker with ClassMocker {
  "JobParser" should {
    val attributes = Map("a" -> 1)
    val jobMap = Map("Job" -> attributes)
    val jobParser = new fake.JobParser

    "when a job parses successfully" >> {
      jobParser(Json.build(jobMap).toString).toMap mustEqual attributes
    }

    "when a job fails to parse" >> {
      jobParser("gobbledygook") must throwA[UnparsableJobException]
    }
  }
}
