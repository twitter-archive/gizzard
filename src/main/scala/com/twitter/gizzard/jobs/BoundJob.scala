package com.twitter.gizzard.jobs

import scala.reflect.Manifest


trait UnboundJob[E] extends Schedulable {
  def apply(environment: E)
}

class BoundJobParser[E](bindingEnvironment: E)(implicit manifest: Manifest[E]) extends JobParser {
  def apply(json: Map[String, Map[String, Any]]) = {
    val (key, attributes) = json.toList.first
    // XXX: hack for now
    val jankykey = key.replace("service.flock.edges", "flockdb").replace("service.flock.groups", "glock.groups")
    val jobClass = Class.forName(jankykey).asInstanceOf[Class[UnboundJob[E]]]
    val unboundJob = jobClass.getConstructor(classOf[Map[String, AnyVal]]).newInstance(attributes)
    new BoundJob(unboundJob, bindingEnvironment)
  }
}

case class BoundJob[E](unboundJob: UnboundJob[E], environment: E)(implicit manifest: Manifest[E]) extends SchedulableProxy(unboundJob) with Job {
  def apply() { unboundJob(environment) }
}
