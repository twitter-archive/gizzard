package com.twitter.gizzard.nameserver

import java.util.{LinkedList => JLinkedList}
import java.nio.ByteBuffer
import com.twitter.rpcclient.LoadBalancingChannel
import com.twitter.util.Duration
import scheduler.JsonJob
import thrift.{JobInjector, JobInjectorClient}

class ClusterBlockedException(cluster: String, cause: Throwable)
extends Exception("Job replication to cluster '" + cluster + "' is blocked.", cause) {
  def this(cluster: String) = this(cluster, null)
}

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

  private val clients = Map(hostMap.flatMap { case (c, hs) =>
    var blocked = false
    val onlineHosts = hs.filter(_.status match {
      case HostStatus.Normal     => true
      case HostStatus.Blocked    => { blocked = true; false }
      case HostStatus.Blackholed => false
    })

    if (onlineHosts.isEmpty) {
      if (blocked) Seq(c -> new BlockedJobRelayCluster(c)) else Seq()
    } else {
      Seq(c -> new JobRelayCluster(onlineHosts, priority, framed, timeout))
    }
  }.toSeq: _*)

  val clusters = clients.keySet

  def apply(cluster: String) = clients.getOrElse(cluster, NullJobRelayCluster)
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

object NullJobRelayCluster extends JobRelayCluster(Seq(), 0, false, new Duration(0)) {
  override val client = null
  override def apply(jobs: Iterable[String]) = ()
}

class BlockedJobRelayCluster(cluster: String) extends JobRelayCluster(Seq(), 0, false, new Duration(0)) {
  override val client = null
  override def apply(jobs: Iterable[String]) { throw new ClusterBlockedException(cluster) }
}
