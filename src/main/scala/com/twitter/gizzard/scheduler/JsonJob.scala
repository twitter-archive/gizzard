package com.twitter.gizzard.scheduler

import com.twitter.ostrich.{StatsProvider, W3CStats}
import org.codehaus.jackson.map.ObjectMapper
import net.lag.logging.Logger
import gizzard.proxy.LoggingProxy
import java.util.{Map => JMap, List => JList}

class UnparsableJsonException(s: String, cause: Throwable) extends Exception(s, cause)

/**
 * A Job that can encode itself as a json-formatted string. The encoding will be a single-element
 * map containing 'className' => 'toMap', where 'toMap' should return a map of key/values from the
 * job. The default 'className' is the job's java/scala class name.
 */
object JsonJob {
  val mapper = new ObjectMapper
}

trait JsonJob extends Job {
  def toMap: Map[String, Any]

  def shouldReplicate = true
  def className = getClass.getName

  def toJson = {
    def json = toMap ++ Map("error_count" -> errorCount, "error_message" -> errorMessage)
    val javaMap = deepConvert(Map(className -> json))
    JsonJob.mapper.writeValueAsBytes(javaMap)
  }

  private def deepConvert(scalaMap: Map[String, Any]): JMap[String, Any] = {
    val map = new java.util.LinkedHashMap[String, Any]()
    scalaMap.map { case (k, v) =>
      v match {
        case m: Map[String, Any] => map.put(k, deepConvert(m))
        case a: Iterable[Any] => map.put(k, deepConvert(a))
        case v => map.put(k, v)
      }
    }
    map
  }

  private def deepConvert(scalaIterable: Iterable[Any]): JList[Any] = {
    val list = new java.util.LinkedList[Any]()
    scalaIterable.map { v =>
      v match {
        case m: Map[String, Any] => list.add(deepConvert(m))
        case a: Iterable[Any] => list.add(deepConvert(a))
        case v => list.add(v)
      }
    }
    list
  }

  override def toString = new String(toJson, "UTF-8")
}

/**
 * A NestedJob that can be encoded in json.
 */
class JsonNestedJob(jobs: Iterable[JsonJob]) extends NestedJob[JsonJob](jobs) with JsonJob {
  def toMap: Map[String, Any] = Map("tasks" -> taskQueue.map { task => Map(task.className -> task.toMap) })
  //override def toString = toJson
}

/**
 * A JobConsumer that encodes JsonJobs into a string and logs them at error level.
 */
class JsonJobLogger(logger: Logger) extends JobConsumer[JsonJob] {
  def put(job: JsonJob) = logger.error(job.toString)
}

class LoggingJsonJobParser(
  jsonJobParser: JsonJobParser, stats: StatsProvider, logger: W3CStats)
  extends JsonJobParser {

  def apply(json: Map[String, Any]): JsonJob = {
    val job = jsonJobParser(json)
    LoggingProxy(stats, logger, job.loggingName, Set("apply"), job)
  }
}

/**
 * A parser that can reconstitute a JsonJob from a map of key/values. Usually registered with a
 * JsonCodec.
 */
trait JsonJobParser {
  def parse(json: Map[String, Any]): JsonJob = {
    val errorCount = json.getOrElse("error_count", 0).asInstanceOf[Int]
    val errorMessage = json.getOrElse("error_message", "(none)").asInstanceOf[String]

    val job = apply(json)
    job.errorCount   = errorCount
    job.errorMessage = errorMessage
    job
  }

  def apply(json: Map[String, Any]): JsonJob
}

class JsonNestedJobParser(codec: JsonCodec) extends JsonJobParser {
  def apply(json: Map[String, Any]): JsonJob = {
    val taskJsons = json("tasks").asInstanceOf[Iterable[Map[String, Any]]]
    new JsonNestedJob(taskJsons.map(codec.inflate))
  }
}
