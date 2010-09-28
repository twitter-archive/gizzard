package com.twitter.gizzard.scheduler

import com.twitter.json.{Json, JsonException}
import net.lag.logging.Logger

class UnparsableJsonException(s: String, cause: Throwable) extends Exception(s, cause)

trait JsonJob extends Job {
  def toMap: Map[String, Any]

  def className = getClass.getName

  def toJson = {
    def json = toMap ++ Map("error_count" -> errorCount, "error_message" -> errorMessage)
    Json.build(Map(className -> json)).toString
  }
}

class JsonNestedJob[J <: JsonJob](jobs: Iterable[J]) extends NestedJob[J](jobs) with JsonJob {
  def toMap = Map("tasks" -> taskQueue.map { task => Map(task.className -> task.toMap) })
}

class JsonJobLogger[J <: JsonJob](logger: Logger) extends JobConsumer[J] {
  def put(job: J) = logger.error(job.toJson)
}

trait JsonJobParser[J <: JsonJob] {
  type Environment
  val environment: Environment

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
