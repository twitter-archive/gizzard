package com.twitter.gizzard.jobs

import scala.reflect.Manifest

trait UnboundJobParser[E] extends (Map[String, Any] => UnboundJob[E])

trait UnboundJob[E] extends Schedulable {
  def apply(environment: E)
}

class BoundJobParser[E](unboundJobParser: UnboundJobParser[E], bindingEnvironment: E) extends JobParser {
  def apply(json: Map[String, Map[String, Any]]) = {
    val attributes = json.toList.first._2
    new BoundJob(unboundJobParser(attributes), bindingEnvironment)
  }
}

case class BoundJob[E](unboundJob: UnboundJob[E], environment: E) extends SchedulableProxy(unboundJob) with Job {
  def apply() { unboundJob(environment) }
}
