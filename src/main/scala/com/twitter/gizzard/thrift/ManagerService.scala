package com.twitter.gizzard.thrift

import com.twitter.logging.Logger
import com.twitter.util.Future
import com.twitter.gizzard.shards._
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.nameserver._
import com.twitter.gizzard.thrift.conversions._

class ManagerService(
  val serverName: String,
  val thriftPort: Int,
  nameServer: NameServer,
  shardManager: ShardManager,
  adminJobManager: AdminJobManager,
  remoteClusterManager: RemoteClusterManager,
  scheduler: PrioritizingJobScheduler)
extends Manager.ThriftServer {

  def wrapEx[A](f: => Future[A]) = {
    val rv = try { f } catch { case e => Future.exception(e) }

    rv rescue {
      case e => {
        log.error(e, "Exception in Gizzard ManagerService: %s", e)
        Future.exception(new GizzardException(e.getMessage))
      }
    }
  }

  def reloadUpdatedForwardings() = wrapEx {
    nameServer.reloadUpdatedForwardings()
    Future.Done
  }

  def reloadConfig() = wrapEx {
    nameServer.reload()
    remoteClusterManager.reload()
    Future.Done
  }

  def findCurrentForwarding(tableId: Int, id: Long) = wrapEx {
    Future(nameServer.findCurrentForwarding(tableId, id).shardInfo)
  }

  // Shard Tree Management

  // XXX: must be nameserver, in order to materialize. odd exception
  def createShard(shard: ShardInfo) = wrapEx {
    Future(shardManager.createAndMaterializeShard(shard))
  }

  def deleteShard(id: ShardId) = wrapEx {
    Future(shardManager.deleteShard(id))
  }


  def addLink(upId: ShardId, downId: ShardId, weight: Int) = wrapEx {
    Future(shardManager.addLink(upId, downId, weight))
  }

  def removeLink(upId: ShardId, downId: ShardId) = wrapEx {
    Future(shardManager.removeLink(upId, downId))
  }


  def setForwarding(forwarding: Forwarding) = wrapEx {
    Future(shardManager.setForwarding(forwarding))
  }

  def replaceForwarding(oldId: ShardId, newId: ShardId) = wrapEx {
    Future(shardManager.replaceForwarding(oldId, newId))
  }

  def removeForwarding(forwarding: Forwarding) = wrapEx {
    Future(shardManager.removeForwarding(forwarding))
  }


  def getShard(id: ShardId) = wrapEx {
    Future(shardManager.getShard(id))
  }

  def shardsForHostname(hostname: String) = wrapEx {
    Future(shardManager.shardsForHostname(hostname))
  }

  def getBusyShards() = wrapEx {
    Future(shardManager.getBusyShards())
  }


  def listUpwardLinks(id: ShardId) = wrapEx {
    Future(shardManager.listUpwardLinks(id))
  }

  def listDownwardLinks(id: ShardId) = wrapEx {
    Future(shardManager.listDownwardLinks(id))
  }


  def getForwarding(tableId: Int, baseId: Long) = wrapEx {
    Future(shardManager.getForwarding(tableId, baseId))
  }


  def getForwardingForShard(id: ShardId) = wrapEx {
    Future(shardManager.getForwardingForShard(id))
  }

  def getForwardings() = wrapEx {
    Future(shardManager.getForwardings())
  }

  def listHostnames() = wrapEx {
    Future(shardManager.listHostnames)
  }

  def markShardBusy(id: ShardId, busy: Int) = wrapEx {
    Future(shardManager.markShardBusy(id, busy))
  }

  def listTables() = wrapEx {
    Future(shardManager.listTables)
  }

  def dumpNameserver(tableIds: Seq[Int]) = wrapEx {
    Future(shardManager.dumpStructure(tableIds))
  }

  def copyShard(shardIds: Seq[ShardId]) = wrapEx {
    Future(adminJobManager.scheduleCopyJob(shardIds))
  }

  def repairShard(shardIds: Seq[ShardId]) = wrapEx {
    Future(adminJobManager.scheduleRepairJob(shardIds))
  }

  def diffShards(shardIds: Seq[ShardId]) = wrapEx {
    Future(adminJobManager.scheduleDiffJob(shardIds))
  }

  // Job Scheduler Management

  def retryErrors()  = wrapEx(Future(scheduler.retryErrors()))
  def stopWrites()   = wrapEx(Future(scheduler.pause()))
  def resumeWrites() = wrapEx(Future(scheduler.resume()))

  def retryErrorsFor(priority: Int)  = wrapEx(Future(scheduler(priority).retryErrors()))
  def stopWritesFor(priority: Int)   = wrapEx(Future(scheduler(priority).pause()))
  def resumeWritesFor(priority: Int) = wrapEx(Future(scheduler(priority).resume()))
  def isWriting(priority: Int)       = wrapEx(Future(!scheduler(priority).isShutdown))
  def queueSize(priority: Int)       = wrapEx(Future(scheduler(priority).size))
  def errorQueueSize(priority: Int)  = wrapEx(Future(scheduler(priority).errorSize))

  def addFanout(suffix: String)    = wrapEx(Future(scheduler.addFanout(suffix)))
  def removeFanout(suffix: String) = wrapEx(Future(scheduler.removeFanout(suffix)))
  def listFanout()                 = wrapEx(Future(scheduler.listFanout().toSeq))

  def addFanoutFor(priority: Int, suffix: String)    = wrapEx(Future(scheduler(priority).addFanout(suffix)))
  def removeFanoutFor(priority: Int, suffix: String) = wrapEx(Future(scheduler(priority).removeFanout(suffix)))


  // Remote Host Cluster Management

  def addRemoteHost(host: Host) = wrapEx {
    Future(remoteClusterManager.addRemoteHost(host))
  }
  def removeRemoteHost(hostname: String, port: Int) = wrapEx {
    Future(remoteClusterManager.removeRemoteHost(hostname, port))
  }
  def setRemoteHostStatus(hostname: String, port: Int, status: HostStatus) = wrapEx {
    Future(remoteClusterManager.setRemoteHostStatus(hostname, port, status))
  }
  def setRemoteClusterStatus(cluster: String, status: HostStatus) = wrapEx {
    Future(remoteClusterManager.setRemoteClusterStatus(cluster, status))
  }

  def getRemoteHost(hostname: String, port: Int) = wrapEx {
    Future(remoteClusterManager.getRemoteHost(hostname, port))
  }

  def listRemoteClusters() = wrapEx(Future(remoteClusterManager.listRemoteClusters))
  def listRemoteHosts() = wrapEx(Future(remoteClusterManager.listRemoteHosts))

  def listRemoteHostsInCluster(cluster: String) = wrapEx {
    Future(remoteClusterManager.listRemoteHosts)
  }
}
