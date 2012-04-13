package com.twitter.gizzard.scheduler

import com.twitter.ostrich.stats.{StatsProvider, W3CStats}
import com.twitter.logging.Logger
import com.twitter.gizzard.proxy.LoggingProxy
import com.twitter.gizzard.util.Json


class UnparsableJsonException(s: String, cause: Throwable) extends Exception(s, cause) {
  def this(s: String) = this(s, null)
}

/**
 * A Job that can encode itself as a json-formatted string. The encoding will be a single-element
 * map containing 'className' => 'toMap', where 'toMap' should return a map of key/values from the
 * job. The default 'className' is the job's java/scala class name.
 */
trait JsonJob {
  @throws(classOf[Exception])
  def apply(): Unit

  var nextJob: Option[JsonJob] = None
  var errorCount: Int = 0
  var errorMessage: String = "(none)"
  private[this] var replicated: Boolean = false

  def loggingName = {
    val className = getClass.getName
    className.lastIndexOf('.') match {
      case -1 => className
      case n => className.substring(n + 1)
    }
  }

  def toMap: Map[String, Any]

  def shouldReplicate = true
  def wasReplicated = replicated
  def setReplicated() { replicated = true }
  def className = getClass.getName

  def toJsonBytes = {
    Json.encode(Map(className -> (toMap ++ Map("error_count" -> errorCount,
                                               "error_message" -> errorMessage,
                                               "wasReplicated" -> replicated))))
  }

  def toJson = new String(toJsonBytes, "UTF-8")

  override def toString = toJson
}

/**
 * A NestedJob that can be encoded in json.
 */
class JsonNestedJob(jobs: Iterable[JsonJob]) extends NestedJob(jobs) with JsonJob {
  def toMap: Map[String, Any] = Map("tasks" -> taskQueue.map { task => Map(task.className -> task.toMap) })
  override def toString = toJson
}

/**
 * A parser that can reconstitute a JsonJob from a map of key/values. Usually registered with a
 * JsonCodec.
 */
trait JsonJobParser {
  def parse(json: Map[String, Any]): JsonJob = {
    val errorCount = json.getOrElse("error_count", 0).asInstanceOf[Int]
    val errorMessage = json.getOrElse("error_message", "(none)").asInstanceOf[String]
    val replicated = json.getOrElse("replicated", false).asInstanceOf[Boolean]

    val job = apply(json)
    job.errorCount   = errorCount
    job.errorMessage = errorMessage
    if (replicated) job.setReplicated()
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

class BadJsonJob extends JsonJob {
  errorCount = Integer.MAX_VALUE - 1
  errorMessage = "Bad Job"
  override def shouldReplicate = false
  override def toMap: Map[String, Any] = Map()

  override def apply() {
    throw new UnparsableJsonException("Bad job")
  }
}

class BadJsonJobParser(codec: JsonCodec) extends JsonJobParser {
  def apply(json: Map[String, Any]): JsonJob = new BadJsonJob()
}
