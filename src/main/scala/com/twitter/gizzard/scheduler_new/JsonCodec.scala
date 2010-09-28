package com.twitter.gizzard.scheduler

import scala.collection.mutable
import scala.util.matching.Regex
import com.twitter.json.Json
import net.lag.logging.Logger

class JsonCodec[J <: JsonJob](unparsableJobHandler: Array[Byte] => Unit) extends Codec[J] {
  private val log = Logger.get(getClass.getName)
  private val processors = mutable.Map.empty[Regex, JsonJobParser[J]]

  def +=(item: (Regex, JsonJobParser[J])) = processors += item
  def +=(r: Regex, p: JsonJobParser[J]) = processors += ((r, p))

  def flatten(job: J): Array[Byte] = job.toJson.getBytes

  def inflate(data: Array[Byte]): J = {
    try {
      Json.parse(new String(data)) match {
        case json: Map[_, _] =>
          assert(json.size == 1)
          inflate(json.asInstanceOf[Map[String, Any]])
      }
    } catch {
      case e =>
        log.error(e, "Unparsable JsonJob; dropping: " + e.toString)
        unparsableJobHandler(data)
        throw new UnparsableJsonException("Unparsable json", e)
    }
  }

  def inflate(json: Map[String, Any]): J = {
    val (jobType, attributes) = json.toList.first
    val (_, processor) = processors.find { case (processorRegex, _) =>
      processorRegex.findFirstIn(jobType).isDefined
    }.getOrElse {
      throw new UnparsableJsonException("Can't find matching processor for '%s' in %s".format(jobType, processors), null)
    }
    try {
      processor.parse(this, attributes.asInstanceOf[Map[String, Any]]).asInstanceOf[J]
    } catch {
      case e =>
        throw new UnparsableJsonException("Processor '%s' blew up: %s".format(jobType, e.toString), e)
    }
  }
}
