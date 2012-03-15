package com.twitter.gizzard
package scheduler

import scala.collection.mutable
import scala.util.matching.Regex
import com.twitter.logging.Logger
import com.twitter.gizzard.util.Json


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
class JsonCodec(unparsableJobHandler: Array[Byte] => Unit) {

  protected val log = Logger.get(getClass.getName)
  protected val exceptionLog = Logger.get("exception")
  protected val processors = {
    val p = mutable.Map.empty[Regex, JsonJobParser]
    p += (("JsonNestedJob".r, new JsonNestedJobParser(this)))
    p += (("ReplicatedJob".r, new ReplicatedJobParser(this)))

    // for backward compat:
    p += (("JobWithTasks".r, new JsonNestedJobParser(this)))
    p += (("SchedulableWithTasks".r, new JsonNestedJobParser(this)))
    p += (("ReplicatingJob".r, new BadJsonJobParser(this)))
    p += (("BadJsonJob".r, new BadJsonJobParser(this)))
    p
  }

  def innerCodec: JsonCodec = this

  def +=(item: (Regex, JsonJobParser)) = processors += item
  def +=(r: Regex, p: JsonJobParser) = processors += ((r, p))

  def flatten(job: JsonJob): Array[Byte] = job.toJsonBytes

  def inflate(data: Array[Byte]): JsonJob = {
    try {
      inflate(Json.decode(data))
    } catch {
      case e => {
        unparsableJobHandler(data)
        throw new UnparsableJsonException("Unparsable json", e)
      }
    }
  }

  def inflate(json: Map[String, Any]): JsonJob = {
    val (jobType, attributes) = json.toList.head
    val processor = processors.find { case (processorRegex, _) =>
      processorRegex.findFirstIn(jobType).isDefined
    }.map { case (_, processor) => processor }.getOrElse {
      throw new UnparsableJsonException("Can't find matching processor for '%s' in %s".format(jobType, processors), null)
    }

    try {
      processor.parse(attributes.asInstanceOf[Map[String, Any]])
    } catch {
      case e =>
        throw new UnparsableJsonException("Processor '%s' blew up: %s".format(jobType, e.toString), e)
    }
  }
}

class LoggingJsonCodec(val codec: JsonCodec, conf: config.StatsCollection, unparsable: Array[Byte] => Unit) extends JsonCodec(unparsable) {
  processors.clear()

  private val proxyFactory = conf[JsonJob]("jobs")
  override val innerCodec = codec.innerCodec

  override def +=(item: (Regex, JsonJobParser)) = codec += item
  override def +=(r: Regex, p: JsonJobParser)   = codec += ((r, p))
  override def inflate(json: Map[String, Any]): JsonJob = {
    proxyFactory(codec.inflate(json))
  }
}

