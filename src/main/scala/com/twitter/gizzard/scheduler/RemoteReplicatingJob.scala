package com.twitter.gizzard.scheduler

import com.twitter.rpcclient.LoadBalancingChannel
import com.twitter.util.Duration
import thrift.{JobInjector, JobInjectorClient}
import thrift.conversions.Sequences._

import java.util.{LinkedList => JLinkedList}


class ReplicatingJobInjector(
  hosts: Seq[String],
  port: Int,
  priority: Int,
  framed: Boolean,
  timeout: Duration)
extends (Iterable[JsonJob] => Unit) {
  val client = new LoadBalancingChannel(hosts.map(new JobInjectorClient(_, port, framed, timeout)))

  def apply(jobs: Iterable[JsonJob]) {
    val jobList = new JLinkedList[thrift.Job]()

    for (j <- jobs) jobList.add(new thrift.Job(priority, j.toJson.getBytes("UTF-8")))
    client.proxy.inject_jobs(jobList)
  }
}

class ReplicatingJsonCodec(injector: Iterable[JsonJob] => Unit, unparsable: Array[Byte] => Unit)
extends JsonCodec[JsonJob](unparsable) {
  this += ("RemoteReplicatingJob".r -> new RemoteReplicatingJobParser(this, injector))

  override def inflate(json: Map[String, Any]): JsonJob = {
    super.inflate(json) match {
      case j: RemoteReplicatingJob[_] => j
      case job => if (job.shouldReplicate) {
        new RemoteReplicatingJob(injector, List(job))
      } else {
        job
      }
    }
  }
}

class RemoteReplicatingJob[J <: JsonJob](injector: Iterable[JsonJob] => Unit, jobs: Iterable[J], var _shouldReplicate: Boolean)
extends JsonNestedJob(jobs) {
  def this(injector: Iterable[JsonJob] => Unit, jobs: Iterable[J]) = this(injector, jobs, true)

  override def shouldReplicate = _shouldReplicate
  def shouldReplicate_=(v: Boolean) = _shouldReplicate = v

  override def toMap: Map[String, Any] = Map("should_replicate" -> shouldReplicate :: super.toMap.toList: _*)

  // XXX: do all this work in parallel in a future pool.
  override def apply() {
    super.apply()

    if (shouldReplicate) try {
      shouldReplicate = false
      injector(List(this))
    } catch {
      case e: Throwable => {
        shouldReplicate = true
        throw e
      }
    }
  }
}

class RemoteReplicatingJobParser[J <: JsonJob](codec: JsonCodec[J], injector: Iterable[JsonJob] => Unit)
extends JsonNestedJobParser(codec) {

  override def apply(json: Map[String, Any]) = {
    val shouldReplicate = json("should_replicate").asInstanceOf[Boolean]
    val nestedJob       = super.apply(json).asInstanceOf[JsonNestedJob[J]]

    new RemoteReplicatingJob(injector, nestedJob.jobs, shouldReplicate).asInstanceOf[J]
  }
}
