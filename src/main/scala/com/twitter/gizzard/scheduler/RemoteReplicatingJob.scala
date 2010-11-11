package com.twitter.gizzard.scheduler

import scala.util.matching.Regex
import com.twitter.rpcclient.LoadBalancingChannel
import com.twitter.util.Duration
import thrift.{JobInjector, JobInjectorClient}
import thrift.conversions.Sequences._

import java.util.{LinkedList => JLinkedList}
import java.nio.ByteBuffer


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

    for (j <- jobs) jobList.add(new thrift.Job(priority, ByteBuffer.wrap(j.toJson.getBytes("UTF-8"))))
    client.proxy.inject_jobs(jobList)
  }
}

class ReplicatingJsonCodec(injector: Iterable[JsonJob] => Unit, unparsable: Array[Byte] => Unit)
extends JsonCodec[JsonJob](unparsable) {
  lazy val innerCodec = {
    val c = new JsonCodec[JsonJob](unparsable)
    c += ("RemoteReplicatingJob".r -> new RemoteReplicatingJobParser(c, injector))
    c
  }

  override def +=(item: (Regex, JsonJobParser[JsonJob])) = innerCodec += item
  override def +=(r: Regex, p: JsonJobParser[JsonJob])   = innerCodec += ((r, p))


  override def inflate(json: Map[String, Any]): JsonJob = {
    innerCodec.inflate(json) match {
      case j: RemoteReplicatingJob[_] => j
      case j => if (j.shouldReplicate) {
        new RemoteReplicatingJob(injector, List(j))
      } else {
        j
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
    if (shouldReplicate) {
      injector(List(new RemoteReplicatingJob[J](injector, jobs, false)))
      shouldReplicate = false
    }

    super.apply()
  }
}

class RemoteReplicatingJobParser[J <: JsonJob](codec: JsonCodec[J], injector: Iterable[JsonJob] => Unit)
extends JsonJobParser[J] {

  override def apply(json: Map[String, Any]) = {
    val shouldReplicate = json("should_replicate").asInstanceOf[Boolean]
    val taskJsons = json("tasks").asInstanceOf[Iterable[Map[String, Any]]]
    val tasks = taskJsons.map { codec.inflate(_) }

    new RemoteReplicatingJob(injector, tasks, shouldReplicate).asInstanceOf[J]
  }
}
