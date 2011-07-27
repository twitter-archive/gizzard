package com.twitter.gizzard.nameserver

import scala.collection.mutable
import com.twitter.logging.Logger
import com.twitter.gizzard.shards.{ShardException, RoutingNode}


class RemoteClusterManager(shard: RoutingNode[RemoteClusterManagerSource], relayFactory: JobRelayFactory) {

  private val log = Logger.get(getClass.getName)

  @volatile
  var jobRelay: JobRelay = NullJobRelay

  def reload() {
    log.info("Loading remote cluster configuration...")

    shard.write.foreach(_.reload)

    val newRemoteClusters = mutable.Map[String, List[Host]]()

    listRemoteHosts.foreach { h =>
      newRemoteClusters += h.cluster -> (h :: newRemoteClusters.getOrElse(h.cluster, Nil))
    }

    jobRelay = relayFactory(newRemoteClusters.toMap)

    log.info("Loading remote cluster configuration is done.")
  }

  def addRemoteHost(h: Host)                                      { shard.write.foreach(_.addRemoteHost(h)) }
  def removeRemoteHost(h: String, p: Int)                         { shard.write.foreach(_.removeRemoteHost(h, p)) }
  def setRemoteHostStatus(h: String, p: Int, s: HostStatus.Value) { shard.write.foreach(_.setRemoteHostStatus(h, p, s)) }
  def setRemoteClusterStatus(c: String, s: HostStatus.Value)      { shard.write.foreach(_.setRemoteClusterStatus(c, s)) }

  def getRemoteHost(h: String, p: Int)    = shard.read.any(_.getRemoteHost(h, p))
  def listRemoteClusters()                = shard.read.any(_.listRemoteClusters())
  def listRemoteHosts()                   = shard.read.any(_.listRemoteHosts())
  def listRemoteHostsInCluster(c: String) = shard.read.any(_.listRemoteHostsInCluster(c))
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
