package com.twitter.gizzard.nameserver

import java.util.{LinkedList => JLinkedList}
import java.nio.ByteBuffer
import java.net.InetSocketAddress
import org.apache.thrift.protocol.TBinaryProtocol
import com.twitter.conversions.time._
import com.twitter.util.Duration
import java.util.logging.Logger
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.gizzard.scheduler.JsonJob
import com.twitter.gizzard.thrift.JobInjector
import com.twitter.gizzard.thrift.{Job => ThriftJob}
import net.lag.kestrel.config.QueueConfig
import net.lag.kestrel.PersistentQueue


class ClusterBlockedException(cluster: String, cause: Throwable)
extends Exception("Job replication to cluster '" + cluster + "' is blocked.", cause) {
  def this(cluster: String) = this(cluster, null)
}

class JobRelayFactory(
  priority: Int,
  timeout: Duration,
  requestTimeout: Duration,
  retries: Int,
  queueConfig: QueueConfig,
  queueRootDir: String)
extends (Map[String, Seq[Host]] => JobRelay) {

//  def this(priority: Int, timeout: Duration) = this(priority, timeout, timeout, 0)

  def apply(hostMap: Map[String, Seq[Host]]) =
    new JobRelay(hostMap, priority, timeout, requestTimeout, retries, queueConfig, queueRootDir)
}

class JobRelay(
  hostMap: Map[String, Seq[Host]],
  priority: Int,
  timeout: Duration,
  requestTimeout: Duration,
  retries: Int,
  queueConfig: QueueConfig,
  queueRootDir: String)
extends (String => JobRelayCluster) {
  
  private val clients = hostMap.flatMap { case (c, hs) =>
    var blocked = false
    val onlineHosts = hs.filter(_.status match {
      case HostStatus.Normal     => true
      case HostStatus.Blocked    => { blocked = true; false }
      case HostStatus.Blackholed => false
    })

    if (onlineHosts.isEmpty) {
      if (blocked) Map(c -> new BlockedJobRelayCluster(c, queueConfig, queueRootDir)) else Map[String, JobRelayCluster]()
    } else {
      Map(c -> new JobRelayCluster(c, onlineHosts, priority, timeout, requestTimeout, retries, queueConfig, queueRootDir))
    }
  }

  val clusters = clients.keySet

  def apply(cluster: String) = clients.getOrElse(cluster, NullJobRelayCluster)

  def enqueue(job: Array[Byte]) {
    clients.values.foreach { _.enqueue(job) }
  }

  def close() {
    clients.foreach { case (cluster, client) => client.close() }
  }
}

class JobRelayCluster(
  name: String,
  hosts: Seq[Host],
  priority: Int,
  timeout: Duration,
  requestTimeout: Duration,
  retries: Int,
  queueConfig: QueueConfig,
  queueRootDir: String)
extends (Iterable[Array[Byte]] => Unit) {
  val service = ClientBuilder()
      .codec(ThriftClientFramedCodec())
      .hosts(hosts.map { h => new InetSocketAddress(h.hostname, h.port) })
      .hostConnectionLimit(2)
      .retries(retries)
      .tcpConnectTimeout(timeout)
      .failureAccrualParams((10, 1.second))
      .requestTimeout(requestTimeout)
      .timeout(timeout)
      .name("JobManagerClient")
      .reportTo(new OstrichStatsReceiver)
      .build()
  val client = new JobInjector.ServiceToClient(service, new TBinaryProtocol.Factory())
  val queue = new PersistentQueue(name, queueRootDir, queueConfig)

  def apply(jobs: Iterable[Array[Byte]]) {
    val jobList = new JLinkedList[ThriftJob]()

    jobs.foreach { j =>
      val tj = new ThriftJob(priority, ByteBuffer.wrap(j))
      tj.setIs_replicated(true)
      jobList.add(tj)
    }

    client.inject_jobs(jobList)()
  }

  def enqueue(job: Array[Byte]) {
    queue.add(job)
  }
  
  def close() {
    service.release()
  }
}

object NullJobRelayFactory extends JobRelayFactory(0, 0.seconds, 0.seconds, 0, null, null) {
  override def apply(h: Map[String, Seq[Host]]) = NullJobRelay
}

object NullJobRelay extends JobRelay(Map(), 0, 0.seconds, 0.seconds, 0, null, null)

object NullJobRelayCluster extends JobRelayCluster(null, Seq(), 0, 0.seconds, 0.seconds, 0, null, null) {
  override val client = null
  override def apply(jobs: Iterable[Array[Byte]]) { }
  override def enqueue(job: Array[Byte]) { }
  override def close() { }
}

class BlockedJobRelayCluster(cluster: String, queueConfig: QueueConfig, queueRootDir: String) extends JobRelayCluster(cluster, Seq(), 0, 0.seconds, 0.seconds, 0, queueConfig, queueRootDir) {
  override val client = null
  override def apply(jobs: Iterable[Array[Byte]]) { throw new ClusterBlockedException(cluster) }
  override def close() { }
}
