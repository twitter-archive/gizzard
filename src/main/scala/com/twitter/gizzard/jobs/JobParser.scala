package com.twitter.gizzard.jobs

import scala.collection.mutable
import scala.reflect.Manifest
import scala.util.matching.Regex
import com.twitter.json.Json
import com.twitter.ostrich.{StatsProvider, W3CStats}


trait JobParser extends (String => Job) {
  @throws(classOf[Exception])
  def apply(data: String) = {
    Json.parse(data) match {
      case job: Map[_, _] =>
        assert(job.size == 1)
        apply(job.asInstanceOf[Map[String, Map[String, Any]]])
    }
  }

  def apply(json: Map[String, Map[String, Any]]): Job  
}

class BindingJobParser[E](bindingEnvironment: E)(implicit manifest: Manifest[E]) extends JobParser {
  def apply(json: Map[String, Map[String, Any]]) = {
    val (key, attributes) = json.toList.first
    val jobClass = Class.forName(key).asInstanceOf[Class[UnboundJob[E]]]
    val unboundJob = jobClass.getConstructor(classOf[Map[String, AnyVal]]).newInstance(attributes)
    new BoundJob(unboundJob, bindingEnvironment)
  }
}

class PolymorphicJobParser extends JobParser {
  private val processors = mutable.Map.empty[Regex, JobParser]

  def +=(item: (Regex, JobParser)) = processors += item

  override def apply(json: Map[String, Map[String, Any]]) = {
    val (jobType, attributes) = json.toList.first
    processors find { p =>
      val (processorRegex, _) = p
      processorRegex.findFirstIn(jobType).isDefined
    } map(_._2(json)) getOrElse {
      throw new Exception("Can't find matching processor for '%s' in %s".format(jobType, processors))
    }
  }
}

class ErrorHandlingJobParser(jobParser: JobParser, config: ErrorHandlingConfig) extends JobParser {
  var errorQueue: MessageQueue = null

  def apply(json: Map[String, Map[String, Any]]) = {
    val (_, attributes) = json.toList.first
    new ErrorHandlingJob(jobParser(json), errorQueue, config, attributes.getOrElse("error_count", 0).asInstanceOf[Int])
  }
}

class LoggingJobParser(w3cStats: W3CStats, jobParser: JobParser) extends JobParser {
  def apply(json: Map[String, Map[String, Any]]) = new LoggingJob(w3cStats, jobParser(json))
}

class JournaledJobParser(jobParser: JobParser, journaller: String => Unit) extends JobParser {
  def apply(json: Map[String, Map[String, Any]]) = new JournaledJob(jobParser(json), journaller)
}

class RecursiveJobParser(jobParser: JobParser) extends JobParser {
  def apply(json: Map[String, Map[String, Any]]) = {
    val (jobClassName, attributes) = json.toList.first
    attributes.get("tasks").map { taskJsons =>
      val tasks = taskJsons.asInstanceOf[Iterable[Map[String, Map[String, Any]]]].map(this(_))
      val jobClass = Class.forName(jobClassName).asInstanceOf[Class[Job]]
      jobClass.getConstructor(classOf[Iterable[Job]]).newInstance(tasks)
    } getOrElse {
      jobParser(json)
    }
  }
}
