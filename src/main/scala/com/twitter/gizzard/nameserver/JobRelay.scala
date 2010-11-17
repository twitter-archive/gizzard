package com.twitter.gizzard.nameserver

import java.util.{LinkedList => JLinkedList}
import java.nio.ByteBuffer
import com.twitter.rpcclient.LoadBalancingChannel
import com.twitter.util.Duration
import scheduler.JsonJob
import thrift.{JobInjector, JobInjectorClient}


class JobRelayFactory(
  port: Int,
  priority: Int,
  framed: Boolean,
  timeout: Duration)
extends (Map[String, Seq[String]] => JobRelay) {
  def apply(hostMap: Map[String, Seq[String]]) =
    new JobRelay(hostMap, port, priority, framed, timeout)
}

class JobRelay(
  hostMap: Map[String, Seq[String]],
  port: Int,
  priority: Int,
  framed: Boolean,
  timeout: Duration)
extends (String => Iterable[JsonJob] => Unit) {

  val clusters = hostMap.keySet

  private val clients = Map(hostMap.map { case (c, hs) =>
    c -> new JobRelayCluster(hs, port, priority, framed, timeout)
  }.toSeq: _*)

  def apply(cluster: String) = clients(cluster)
}

class JobRelayCluster(
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

