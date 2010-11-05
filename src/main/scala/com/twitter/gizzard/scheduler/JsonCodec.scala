package com.twitter.gizzard.scheduler

import scala.collection.mutable
import scala.util.matching.Regex
import com.twitter.json.Json
import net.lag.logging.Logger

/**
 * Codec for json-encoded jobs.
 *
 * A JsonJob can turn itself into a json-encoded string via 'toJson'/'toMap', so encoding is
 * delegated to the JsonJob itself.
 *
 * For decoding, a set of JsonJobParsers are registered with corresponding regular expressions.
 * If the primary key of the json structure matches the expression, the nested map is passed to
 * that parser to turn it into a JsonJob.
 *
 * Jobs that can't be parsed by the json library are handed to 'unparsableJobHandler'.
 */
class JsonCodec[J <: JsonJob](unparsableJobHandler: Array[Byte] => Unit) extends Codec[J] {
  private val log = Logger.get(getClass.getName)
  private val processors = mutable.Map.empty[Regex, JsonJobParser[J]]

  def +=(item: (Regex, JsonJobParser[J])) = processors += item
  def +=(r: Regex, p: JsonJobParser[J]) = processors += ((r, p))

  this += ("JsonNestedJob".r, new JsonNestedJobParser(this))

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
    val processor = processors.find { case (processorRegex, _) =>
      processorRegex.findFirstIn(jobType).isDefined
    }.map { case (_, processor) => processor }.getOrElse {
      throw new UnparsableJsonException("Can't find matching processor for '%s' in %s".format(jobType, processors), null)
    }
    try {
      processor.parse(attributes.asInstanceOf[Map[String, Any]]).asInstanceOf[J]
    } catch {
      case e =>
        throw new UnparsableJsonException("Processor '%s' blew up: %s".format(jobType, e.toString), e)
    }
  }
}
