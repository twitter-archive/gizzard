package com.twitter.gizzard.jobs

import com.twitter.json.{Json, JsonException}


class UnparsableJobException(e: JsonException) extends Exception

trait JobParser extends (String => Job) {
  @throws(classOf[UnparsableJobException])
  def apply(data: String) = {
    try {
      Json.parse(data) match {
        case job: Map[_, _] =>
          assert(job.size == 1)
          apply(job.asInstanceOf[Map[String, Map[String, Any]]])
      }
    } catch {
      case e: JsonException => throw new UnparsableJobException(e)
    }
  }

  def apply(json: Map[String, Map[String, Any]]): Job  
}
