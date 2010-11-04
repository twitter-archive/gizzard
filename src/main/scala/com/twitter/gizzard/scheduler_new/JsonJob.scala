package com.twitter.gizzard.scheduler

import com.twitter.json.{Json, JsonException}
import net.lag.logging.Logger

class UnparsableJsonException(s: String, cause: Throwable) extends Exception(s, cause)

/**
 * A Job that can encode itself as a json-formatted string. The encoding will be a single-element
 * map containing 'className' => 'toMap', where 'toMap' should return a map of key/values from the
 * job. The default 'className' is the job's java/scala class name.
 */
trait JsonJob extends Job {
  def toMap: Map[String, Any]

  def className = getClass.getName

  def toJson = {
    def json = toMap ++ Map("error_count" -> errorCount, "error_message" -> errorMessage)
    Json.build(Map(className -> json)).toString
  }
}

/**
 * A NestedJob that can be encoded in json.
 */
class JsonNestedJob[J <: JsonJob](jobs: Iterable[J]) extends NestedJob[J](jobs) with JsonJob {
  def toMap = Map("tasks" -> taskQueue.map { task => Map(task.className -> task.toMap) })
}

/**
 * A JobConsumer that encodes JsonJobs into a string and logs them at error level.
 */
class JsonJobLogger[J <: JsonJob](logger: Logger) extends JobConsumer[J] {
  def put(job: J) = logger.error(job.toJson)
}

/**
 * A parser that can reconstitute a JsonJob from a map of key/values. Usually registered with a
 * JsonCodec.
 */
trait JsonJobParser[J <: JsonJob] {
  def parse(codec: JsonCodec[J], json: Map[String, Any]): JsonJob = {
    val errorCount = json.getOrElse("error_count", 0).asInstanceOf[Int]
    val errorMessage = json.getOrElse("error_message", "(none)").asInstanceOf[String]

    val job = json.get("tasks").map { taskJsons =>
      val tasks = taskJsons.asInstanceOf[Iterable[Map[String, Any]]].map { codec.inflate(_) }
      new JsonNestedJob(tasks.asInstanceOf[Iterable[J]])
    } getOrElse {
      apply(codec, json)
    }

    job.errorCount = errorCount
    job.errorMessage = errorMessage
    job
  }

  def apply(codec: JsonCodec[J], json: Map[String, Any]): J
}
