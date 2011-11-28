package com.twitter.gizzard.nameserver

import scala.collection.mutable
import com.twitter.logging.Logger
import com.twitter.gizzard.shards.{ShardException, RoutingNode}

class RemoteClusterManagerUninitialized extends ShardException("Please call reload() before operating on the RemoteClusterManager")

class RemoteClusterManager(shard: RoutingNode[RemoteClusterManagerSource], relayFactory: JobRelayFactory) {

  private val log = Logger.get(getClass.getName)
  private var initialized = false

  @volatile
  var jobRelay: JobRelay = NullJobRelay

  def reload() {
    log.info("Loading remote cluster configuration...")

    initialized = true
    shard.write.foreach(_.reload)

    val newRemoteClusters = mutable.Map[String, List[Host]]()

    listRemoteHosts.foreach { h =>
      newRemoteClusters += h.cluster -> (h :: newRemoteClusters.getOrElse(h.cluster, Nil))
    }

    jobRelay = relayFactory(newRemoteClusters.toMap)

    log.info("Loading remote cluster configuration is done.")
  }

  def closeRelay() {
    jobRelay.close()
  }

  def addRemoteHost(h: Host)                                      { shard.write.foreach(_.addRemoteHost(h)) }
  def removeRemoteHost(h: String, p: Int)                         { shard.write.foreach(_.removeRemoteHost(h, p)) }
  def setRemoteHostStatus(h: String, p: Int, s: HostStatus.Value) { shard.write.foreach(_.setRemoteHostStatus(h, p, s)) }
  def setRemoteClusterStatus(c: String, s: HostStatus.Value)      { shard.write.foreach(_.setRemoteClusterStatus(c, s)) }

  def checkInitialized[A](f: => A): A = {
    if (!initialized) throw new RemoteClusterManagerUninitialized
    f
  }

  def getRemoteHost(h: String, p: Int)    = checkInitialized { shard.read.any(_.getRemoteHost(h, p)) }
  def listRemoteClusters()                = checkInitialized { shard.read.any(_.listRemoteClusters()) }
  def listRemoteHosts()                   = checkInitialized { shard.read.any(_.listRemoteHosts()) }
  def listRemoteHostsInCluster(c: String) = checkInitialized { shard.read.any(_.listRemoteHostsInCluster(c)) }
}

trait RemoteClusterManagerSource {
  @throws(classOf[ShardException]) def reload()
  @throws(classOf[ShardException]) def addRemoteHost(h: Host)
  @throws(classOf[ShardException]) def removeRemoteHost(h: String, p: Int)
  @throws(classOf[ShardException]) def setRemoteHostStatus(h: String, p: Int, s: HostStatus.Value)
  @throws(classOf[ShardException]) def setRemoteClusterStatus(c: String, s: HostStatus.Value)

  @throws(classOf[ShardException]) def getRemoteHost(h: String, p: Int): Host
  @throws(classOf[ShardException]) def listRemoteClusters(): Seq[String]
  @throws(classOf[ShardException]) def listRemoteHosts(): Seq[Host]
  @throws(classOf[ShardException]) def listRemoteHostsInCluster(c: String): Seq[Host]
}
