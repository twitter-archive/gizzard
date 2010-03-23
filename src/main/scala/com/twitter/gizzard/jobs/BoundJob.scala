package com.twitter.gizzard.jobs

import scala.reflect.Manifest


trait UnboundJob[E] extends Schedulable {
  def apply(environment: E)
}

class BoundJobParser[E](bindingEnvironment: E)(implicit manifest: Manifest[E]) extends JobParser {
  def apply(json: Map[String, Map[String, Any]]) = {
    val (key, attributes) = json.toList.first
    val jobClass = Class.forName(key).asInstanceOf[Class[UnboundJob[E]]]
    val unboundJob = jobClass.getConstructor(classOf[Map[String, AnyVal]]).newInstance(attributes)
    new BoundJob(unboundJob, bindingEnvironment)
  }
}

case class BoundJob[E](unboundJob: UnboundJob[E], environment: E)(implicit manifest: Manifest[E]) extends SchedulableProxy(unboundJob) with Job {
  def apply() { unboundJob(environment) }
}
