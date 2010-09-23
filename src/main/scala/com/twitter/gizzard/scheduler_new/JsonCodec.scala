package com.twitter.gizzard.scheduler_new

import scala.collection.mutable
import scala.util.matching.Regex
import com.twitter.json.{Json, JsonException}
import net.lag.logging.Logger

class UnparsableJsonException(s: String, cause: Throwable) extends Exception(s, cause)

abstract class JsonJob[E](environment: E) extends Job[E](environment) {
  def toMap: Map[String, Any]

  def className = getClass.getName

  def toJson = {
    def json = toMap ++ Map("error_count" -> errorCount, "error_message" -> errorMessage)
    Json.build(Map(className -> json)).toString
  }
}

trait JsonParser[E, J <: JsonJob[E]] {
  val environment: E

  @throws(classOf[UnparsableJsonException])
  def parse(data: String): J = {
    try {
      Json.parse(data) match {
        case job: Map[_, _] =>
          assert(job.size == 1)
          parse(job.asInstanceOf[Map[String, Any]])
      }
    } catch {
      case e: JsonException =>
        throw new UnparsableJsonException("Unparsable json", e)
    }
  }

  def parse(json: Map[String, Any]): J = {
    val errorCount = json.getOrElse("error_count", 0).asInstanceOf[Int]
    val errorMessage = json.getOrElse("error_message", "(none)").asInstanceOf[String]
    val job = apply(json)
    job.errorCount = errorCount
    job.errorMessage = errorMessage
    job
  }

  def apply(json: Map[String, Any]): J
}

class JsonCodec[E, J <: JsonJob[E]](unparsableJobHandler: Array[Byte] => Unit) {
  private val log = Logger.get(getClass.getName)
  private val processors = mutable.Map.empty[Regex, JsonParser[E, J]]

  def +=(item: (Regex, JsonParser[E, J])) = processors += item
  def +=(r: Regex, p: JsonParser[E, J]) = processors += ((r, p))

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
        throw e
    }
  }

  def inflate(json: Map[String, Any]) = {
    val (jobType, attributes) = json.toList.first
    val (_, processor) = processors.find { case (processorRegex, _) =>
      processorRegex.findFirstIn(jobType).isDefined
    }.getOrElse {
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
