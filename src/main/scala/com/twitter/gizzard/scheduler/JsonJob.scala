package com.twitter.gizzard
package scheduler

import com.twitter.ostrich.{StatsProvider, W3CStats}
import org.codehaus.jackson.map.ObjectMapper
import net.lag.logging.Logger
import java.util.{Map => JMap, List => JList}

import proxy.LoggingProxy


class UnparsableJsonException(s: String, cause: Throwable) extends Exception(s, cause)

/**
 * A Job that can encode itself as a json-formatted string. The encoding will be a single-element
 * map containing 'className' => 'toMap', where 'toMap' should return a map of key/values from the
 * job. The default 'className' is the job's java/scala class name.
 */
object JsonJob {
  val mapper = new ObjectMapper
}

trait JsonJob {
  @throws(classOf[Exception])
  def apply(): Unit

  var nextJob: Option[JsonJob] = None
  var errorCount: Int = 0
  var errorMessage: String = "(none)"

  def loggingName = {
    val className = getClass.getName
    className.lastIndexOf('.') match {
      case -1 => className
      case n => className.substring(n + 1)
    }
  }

  def toMap: Map[String, Any]

  def shouldReplicate = true
  def className = getClass.getName

  def toJsonBytes = {
    def json = toMap ++ Map("error_count" -> errorCount, "error_message" -> errorMessage)
    val javaMap = deepConvert(Map(className -> json))
    JsonJob.mapper.writeValueAsBytes(javaMap)
  }

  def toJson = new String(toJsonBytes, "UTF-8")

  private def deepConvert(scalaMap: Map[String, Any]): JMap[String, Any] = {
    val map = new java.util.LinkedHashMap[String, Any]()
    scalaMap.map { case (k, v) =>
      v match {
        case m: Map[_,_]    => map.put(k, deepConvert(m.asInstanceOf[Map[String,Any]]))
        case a: Iterable[_] => map.put(k, deepConvert(a.asInstanceOf[Iterable[Any]]))
        case v => map.put(k, v)
      }
    }
    map
  }

  private def deepConvert(scalaIterable: Iterable[Any]): JList[Any] = {
    val list = new java.util.LinkedList[Any]()
    scalaIterable.map { v =>
      v match {
        case m: Map[_,_]    => list.add(deepConvert(m.asInstanceOf[Map[String,Any]]))
        case a: Iterable[_] => list.add(deepConvert(a.asInstanceOf[Iterable[Any]]))
        case v => list.add(v)
      }
    }
    list
  }

  override def toString = toJson
}

/**
 * A NestedJob that can be encoded in json.
 */
class JsonNestedJob(jobs: Iterable[JsonJob]) extends NestedJob(jobs) with JsonJob {
  def toMap: Map[String, Any] = Map("tasks" -> taskQueue.map { task => Map(task.className -> task.toMap) })
  //override def toString = toJson
}

/**
 * A JobConsumer that encodes JsonJobs into a string and logs them at error level.
 */
class JsonJobLogger(logger: Logger) extends JobConsumer {
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
