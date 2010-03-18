package com.twitter.gizzard.jobs

import scala.util.matching.Regex
import scala.collection.mutable


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