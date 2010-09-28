package com.twitter.gizzard.scheduler

import com.twitter.json.{Json, JsonException}
import net.lag.logging.Logger

class UnparsableJsonException(s: String, cause: Throwable) extends Exception(s, cause)

trait JsonJob[E] extends Job[E] {
  def toMap: Map[String, Any]

  def className = getClass.getName

  def toJson = {
    def json = toMap ++ Map("error_count" -> errorCount, "error_message" -> errorMessage)
    Json.build(Map(className -> json)).toString
  }
}

class JsonNestedJob[E, J <: JsonJob[E]](environment: E, jobs: Iterable[J]) extends NestedJob[E, J](environment, jobs) with JsonJob[E] {
  def toMap = Map("tasks" -> taskQueue.map { task => Map(task.className -> task.toMap) })
}

class JsonJobLogger[J <: JsonJob[_]](logger: Logger) extends JobConsumer[J] {
  def put(job: J) = logger.error(job.toJson)
}

trait JsonJobParser[E, J <: JsonJob[E]] {
  val environment: E

  def parse(codec: JsonCodec[E, J], json: Map[String, Any]): JsonJob[E] = {
    val errorCount = json.getOrElse("error_count", 0).asInstanceOf[Int]
    val errorMessage = json.getOrElse("error_message", "(none)").asInstanceOf[String]

    val job = json.get("tasks").map { taskJsons =>
      val tasks = taskJsons.asInstanceOf[Iterable[Map[String, Any]]].map { codec.inflate(_) }
      new JsonNestedJob(environment, tasks)
    } getOrElse {
      apply(codec, json)
    }

    job.errorCount = errorCount
    job.errorMessage = errorMessage
    job
  }

  def apply(codec: JsonCodec[E, J], json: Map[String, Any]): J
}
