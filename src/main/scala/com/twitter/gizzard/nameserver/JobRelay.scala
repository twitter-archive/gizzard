package com.twitter.gizzard.nameserver

import java.util.{LinkedList => JLinkedList}
import java.nio.ByteBuffer
import com.twitter.rpcclient.LoadBalancingChannel
import com.twitter.util.Duration
import scheduler.JsonJob
import thrift.{JobInjector, JobInjectorClient}


class JobRelayFactory(
  priority: Int,
  framed: Boolean,
  timeout: Duration)
extends (Map[String, Seq[Host]] => JobRelay) {
  def apply(hostMap: Map[String, Seq[Host]]) =
    new JobRelay(hostMap, priority, framed, timeout)
}

class JobRelay(
  hostMap: Map[String, Seq[Host]],
  priority: Int,
  framed: Boolean,
  timeout: Duration)
extends (String => JobRelayCluster) {

  val clusters = hostMap.keySet

  private val clients = Map(hostMap.map { case (c, hs) =>
    c -> new JobRelayCluster(hs, priority, framed, timeout)
  }.toSeq: _*)

  def apply(cluster: String) = clients(cluster)
}

class JobRelayCluster(
  hosts: Seq[Host],
  priority: Int,
  framed: Boolean,
  timeout: Duration)
extends (Iterable[String] => Unit) {
  val client = new LoadBalancingChannel(hosts.map(h => new JobInjectorClient(h.hostname, h.port, framed, timeout)))

  def apply(jobs: Iterable[String]) {
    val jobList = new JLinkedList[thrift.Job]()

    jobs.foreach { j =>
      val tj = new thrift.Job(priority, ByteBuffer.wrap(j.getBytes("UTF-8")))
      tj.setIs_replicated(true)
      jobList.add(tj)
    }

    client.proxy.inject_jobs(jobList)
  }
}

object NullJobRelayFactory extends JobRelayFactory(0, false, new Duration(0)) {
  override def apply(h: Map[String, Seq[Host]]) = NullJobRelay
}

object NullJobRelay extends JobRelay(Map(), 0, false, new Duration(0))
