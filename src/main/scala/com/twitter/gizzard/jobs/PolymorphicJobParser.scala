package com.twitter.gizzard.jobs

import scala.util.matching.Regex
import scala.collection.mutable


class PolymorphicJobParser extends JobParser {
  private val processors = mutable.Map.empty[Regex, JobParser]

  def +=(item: (Regex, JobParser)) = processors += item
  def +=(r: Regex, p: JobParser) = processors += ((r, p))

  override def apply(json: Map[String, Map[String, Any]]) = {
    val (jobType, attributes) = json.toList.first
    val regexpAndProcessor = processors find { p =>
      val (processorRegex, _) = p
      processorRegex.findFirstIn(jobType).isDefined
    } getOrElse {
      throw new UnparsableJobException("Can't find matching processor for '%s' in %s".format(jobType, processors), null)
    }
    try {
      val (_, processor) = regexpAndProcessor
      processor(json)
    } catch {
      case e => throw new UnparsableJobException("Processor blew up: " + e.toString, e)
    }
  }
}
