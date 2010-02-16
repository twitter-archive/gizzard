package com.twitter.gizzard.jobs

import net.lag.configgy.Configgy
import net.lag.logging.{Logger, Level, FileFormatter, StringHandler}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


class FakeJob(protected val attributes: Map[String, AnyVal]) extends UnboundJob[Int] {
  def toMap = attributes

  override def equals(that: Any) = that match {
    case that: FakeJob => attributes == that.attributes
    case _ => false
  }

  def apply(i: Int) {}
}

object JobParserSpec extends Specification with JMocker with ClassMocker {
  "BindingJobParser" should {
    "apply" in {
      val job = new FakeJob(Map("a" -> 1, "error_count" -> 0))

      (new BindingJobParser(1)).apply(job.toJson) mustEqual new BoundJob(job, 1)
    }
  }

  "PolymorphicJobParser" should {
    "+= & apply" in {
      val multiParser = new PolymorphicJobParser
      val a = mock[JobParser]
      val b = mock[JobParser]
      val aJson = Map("a" -> Map("b" -> 1))
      val bJson = Map("b" -> Map("b" -> 1))
      multiParser += ("a".r, a)
      multiParser += ("b".r, b)

      expect { one(a).apply(aJson) }
      multiParser(aJson)

      expect { one(b).apply(bJson) }
      multiParser(bJson)
    }
  }

  "RecursiveJobParser" should {
    "apply" in {
      val job = mock[Job]
      val jobParser = mock[JobParser]
      val taskJson = Map("Bar" -> Map("a" -> 1))
      val recursiveJobParser = new RecursiveJobParser(jobParser)
      expect {
        one(jobParser).apply(taskJson) willReturn job
      }
      val result = recursiveJobParser(Map("com.twitter.gizzard.jobs.JobWithTasks" -> Map("tasks" ->
        List(taskJson)
      )))
      result mustEqual new JobWithTasks(List(job))
    }
  }
}
