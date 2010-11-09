package com.twitter.gizzard.scheduler

import com.twitter.rpcclient.{Client, LoadBalancingChannel}
import com.twitter.util.TimeConversions._
import thrift.{JobInjector, JobInjectorClient}
import thrift.conversions.Sequences._


class RemoteReplicatingJob[J <: JsonJob](client: Client[JobInjector.Iface], priority: Int, jobs: Iterable[J], var replicated: Boolean) extends JsonNestedJob(jobs) {
  override def toMap: Map[String, Any] = Map("replicated" -> replicated :: super.toMap.toList: _*)

  override def apply() {
    // XXX: this requires an allocation of a new list. we should thread a list through.
    client.proxy.inject_jobs(jobs.map { job => new thrift.Job(priority, job.toJson.getBytes("UTF-8")) }.toList.toJavaList)
    replicated = true
    super.apply()
  }
}

class RemoteReplicatingJobParser[J <: JsonJob](codec: JsonCodec[J], hosts: Seq[String], port: Int, priority: Int) extends JsonNestedJobParser(codec) {
  val client = new LoadBalancingChannel(hosts.map { new JobInjectorClient(_, port, true, 1.second) } )

  override def apply(json: Map[String, Any]) = {
    val replicated = json("replicated").asInstanceOf[Boolean]
    val nestedJob = super.apply(json).asInstanceOf[JsonNestedJob[J]]

    new RemoteReplicatingJob(client, priority, nestedJob.jobs, replicated).asInstanceOf[J]
  }
}
