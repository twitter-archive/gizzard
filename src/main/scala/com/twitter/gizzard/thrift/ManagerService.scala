package com.twitter.gizzard.thrift

import java.util.{List => JList}
import scala.reflect.Manifest
import scala.collection.JavaConversions._
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.thrift.conversions.Busy._
import com.twitter.gizzard.thrift.conversions.LinkInfo._
import com.twitter.gizzard.thrift.conversions.ShardId._
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import com.twitter.gizzard.thrift.conversions.Forwarding._
import com.twitter.gizzard.thrift.conversions.Host._
import com.twitter.gizzard.shards._
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.nameserver._
import com.twitter.logging.Logger


class ManagerService(
  nameServer: NameServer,
  shardManager: ShardManager,
  adminJobManager: AdminJobManager,
  remoteClusterManager: RemoteClusterManager,
  scheduler: PrioritizingJobScheduler)
extends Manager.Iface {

  val log = Logger.get(getClass.getName)

  def wrapEx[A](f: => A): A = try { f } catch {
    case ex: Throwable =>
      log.error(ex, "Exception in Gizzard ManagerService: %s", ex)
      throw new GizzardException(ex.getMessage)
  }

  def reload_updated_forwardings() = wrapEx {
    nameServer.reloadUpdatedForwardings()
  }
  def reload_config() = wrapEx {
    nameServer.reload()
    remoteClusterManager.reload()
  }

  def find_current_forwarding(tableId: Int, id: Long) = {
    wrapEx(nameServer.findCurrentForwarding(tableId, id).shardInfo.toThrift)
  }

  // Shard Tree Management

  // XXX: must be nameserver, in order to materialize. odd exception
  def create_shard(shard: ShardInfo) = wrapEx(shardManager.createAndMaterializeShard(shard.fromThrift))

  def delete_shard(id: ShardId)      = wrapEx(shardManager.deleteShard(id.fromThrift))


  def add_link(upId: ShardId, downId: ShardId, weight: Int) = {
    wrapEx(shardManager.addLink(upId.fromThrift, downId.fromThrift, weight))
  }
  def remove_link(upId: ShardId, downId: ShardId) = {
    wrapEx(shardManager.removeLink(upId.fromThrift, downId.fromThrift))
  }


  def set_forwarding(forwarding: Forwarding) = {
    wrapEx(shardManager.setForwarding(forwarding.fromThrift))
  }
  def replace_forwarding(oldId: ShardId, newId: ShardId) = {
    wrapEx(shardManager.replaceForwarding(oldId.fromThrift, newId.fromThrift))
  }
  def remove_forwarding(forwarding: Forwarding) = {
    wrapEx(shardManager.removeForwarding(forwarding.fromThrift))
  }


  def get_shard(id: ShardId): ShardInfo = {
    wrapEx(shardManager.getShard(id.fromThrift).toThrift)
  }
  def shards_for_hostname(hostname: String): JList[ShardInfo] = {
    wrapEx(shardManager.shardsForHostname(hostname).map(_.toThrift))
  }
  def get_busy_shards(): JList[ShardInfo] = {
    wrapEx(shardManager.getBusyShards().map(_.toThrift))
  }


  def list_upward_links(id: ShardId): JList[LinkInfo] = {
    wrapEx(shardManager.listUpwardLinks(id.fromThrift).map(_.toThrift))
  }
  def list_downward_links(id: ShardId): JList[LinkInfo] = {
    wrapEx(shardManager.listDownwardLinks(id.fromThrift).map(_.toThrift))
  }


  def get_forwarding(tableId: Int, baseId: Long) = {
    wrapEx(shardManager.getForwarding(tableId, baseId).toThrift)
  }


  def get_forwarding_for_shard(id: ShardId) = {
    wrapEx(shardManager.getForwardingForShard(id.fromThrift).toThrift)
  }
  def get_forwardings(): JList[Forwarding] = {
    wrapEx(shardManager.getForwardings().map(_.toThrift))
  }

  def list_hostnames() = wrapEx(shardManager.listHostnames)

  def mark_shard_busy(id: ShardId, busy: Int) = {
    wrapEx(shardManager.markShardBusy(id.fromThrift, busy.fromThrift))
  }

  def list_tables(): JList[java.lang.Integer] = wrapEx(shardManager.listTables)

  def dump_nameserver(tableIds: JList[java.lang.Integer]) = wrapEx(shardManager.dumpStructure(tableIds.toList).map(_.toThrift))

  def copy_shard(sourceId: ShardId, destinationId: ShardId) = {
    wrapEx(adminJobManager.scheduleCopyJob(sourceId.fromThrift, destinationId.fromThrift))
  }

  def repair_shard(shardIds: JList[ShardId]) = {
    wrapEx(adminJobManager.scheduleRepairJob(shardIds.toList.map(_.asInstanceOf[ShardId].fromThrift)))
  }

  def diff_shards(shardIds: JList[ShardId]) = {
    wrapEx(adminJobManager.scheduleDiffJob(shardIds.toList.map(_.asInstanceOf[ShardId].fromThrift)))
  }

  // Job Scheduler Management

  def retry_errors()  = wrapEx(scheduler.retryErrors())
  def stop_writes()   = wrapEx(scheduler.pause())
  def resume_writes() = wrapEx(scheduler.resume())

  def retry_errors_for(priority: Int)  = wrapEx(scheduler(priority).retryErrors())
  def stop_writes_for(priority: Int)   = wrapEx(scheduler(priority).pause())
  def resume_writes_for(priority: Int) = wrapEx(scheduler(priority).resume())
  def is_writing(priority: Int)        = wrapEx(!scheduler(priority).isShutdown)
  def queue_size(priority: Int)        = wrapEx(scheduler(priority).size)
  def error_queue_size(priority: Int)  = wrapEx(scheduler(priority).errorSize)

  // Remote Host Cluster Management

  def add_remote_host(host: Host) = {
    wrapEx(remoteClusterManager.addRemoteHost(host.fromThrift))
  }
  def remove_remote_host(hostname: String, port: Int) = {
    wrapEx(remoteClusterManager.removeRemoteHost(hostname, port))
  }
  def set_remote_host_status(hostname: String, port: Int, status: HostStatus) = {
    wrapEx(remoteClusterManager.setRemoteHostStatus(hostname, port, status.fromThrift))
  }
  def set_remote_cluster_status(cluster: String, status: HostStatus) = {
    wrapEx(remoteClusterManager.setRemoteClusterStatus(cluster, status.fromThrift))
  }

  def get_remote_host(hostname: String, port: Int) = {
    wrapEx(remoteClusterManager.getRemoteHost(hostname, port).toThrift)
  }

  def list_remote_clusters(): JList[String] = wrapEx(remoteClusterManager.listRemoteClusters)
  def list_remote_hosts(): JList[Host]      = wrapEx(remoteClusterManager.listRemoteHosts.map(_.toThrift))

  def list_remote_hosts_in_cluster(cluster: String): JList[Host] = {
    wrapEx(remoteClusterManager.listRemoteHosts.map(_.toThrift))
  }
}
