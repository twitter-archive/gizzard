package com.twitter.gizzard
package thrift

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
import com.twitter.gizzard.scheduler.{JsonJob, JobScheduler, PrioritizingJobScheduler, CopyJobFactory}
import com.twitter.gizzard.nameserver._
import net.lag.logging.Logger
import java.util.{List => JList}


class ManagerService[S <: shards.Shard](nameServer: NameServer[S], scheduler: PrioritizingJobScheduler, copier: CopyJobFactory[S], copyPriority: Int) extends Manager.Iface {
  val log = Logger.get(getClass.getName)

  def wrapEx[A](f: => A): A = try { f } catch {
    case ex: Throwable =>
      log.error(ex, "Exception in Gizzard ManagerService: %s", ex)
      throw new thrift.GizzardException(ex.getMessage)
  }

  def reload_updated_forwardings() = wrapEx {
    nameServer.reloadUpdatedForwardings()
  }
  def reload_config() = wrapEx {
    nameServer.reload()
  }

  def rebuild_schema() = wrapEx(nameServer.rebuildSchema())
  def find_current_forwarding(tableId: Int, id: Long) = {
    wrapEx(nameServer.findCurrentForwarding(tableId, id).shardInfo.toThrift)
  }

  // Shard Tree Management

  def create_shard(shard: ShardInfo) = wrapEx(nameServer.createShard(shard.fromThrift))
  def delete_shard(id: ShardId)      = wrapEx(nameServer.deleteShard(id.fromThrift))


  def add_link(upId: ShardId, downId: ShardId, weight: Int) = {
    wrapEx(nameServer.addLink(upId.fromThrift, downId.fromThrift, weight))
  }
  def remove_link(upId: ShardId, downId: ShardId) = {
    wrapEx(nameServer.removeLink(upId.fromThrift, downId.fromThrift))
  }


  def set_forwarding(forwarding: Forwarding) = {
    wrapEx(nameServer.setForwarding(forwarding.fromThrift))
  }
  def replace_forwarding(oldId: ShardId, newId: ShardId) = {
    wrapEx(nameServer.replaceForwarding(oldId.fromThrift, newId.fromThrift))
  }
  def remove_forwarding(forwarding: Forwarding) = {
    wrapEx(nameServer.removeForwarding(forwarding.fromThrift))
  }


  def get_shard(id: ShardId): ShardInfo = {
    wrapEx(nameServer.getShard(id.fromThrift).toThrift)
  }
  def shards_for_hostname(hostname: String): JList[ShardInfo] = {
    wrapEx(nameServer.shardsForHostname(hostname).map(_.toThrift))
  }
  def get_busy_shards(): JList[ShardInfo] = {
    wrapEx(nameServer.getBusyShards().map(_.toThrift))
  }


  def list_upward_links(id: ShardId): JList[LinkInfo] = {
    wrapEx(nameServer.listUpwardLinks(id.fromThrift).map(_.toThrift))
  }
  def list_downward_links(id: ShardId): JList[LinkInfo] = {
    wrapEx(nameServer.listDownwardLinks(id.fromThrift).map(_.toThrift))
  }


  def get_forwarding(tableId: Int, baseId: Long) = {
    wrapEx(nameServer.getForwarding(tableId, baseId).toThrift)
  }


  def get_forwarding_for_shard(id: ShardId) = {
    wrapEx(nameServer.getForwardingForShard(id.fromThrift).toThrift)
  }
  def get_forwardings(): JList[Forwarding] = {
    wrapEx(nameServer.getForwardings().map(_.toThrift))
  }

  def list_hostnames() = wrapEx(nameServer.listHostnames)

  def mark_shard_busy(id: ShardId, busy: Int) = {
    wrapEx(nameServer.markShardBusy(id.fromThrift, busy.fromThrift))
  }

  def list_tables(): JList[java.lang.Integer] = wrapEx(nameServer.listTables)

  def dump_nameserver(tableIds: JList[java.lang.Integer]) = wrapEx(nameServer.dumpStructure(tableIds.toList).map(_.toThrift))

  def copy_shard(shardIds: JList[ShardId]) = {
    wrapEx((scheduler.asInstanceOf[PrioritizingJobScheduler]).put(copyPriority, copier(
      shardIds.toList.map(_.asInstanceOf[ShardId].fromThrift)
    )))
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
    wrapEx(nameServer.addRemoteHost(host.fromThrift))
  }
  def remove_remote_host(hostname: String, port: Int) = {
    wrapEx(nameServer.removeRemoteHost(hostname, port))
  }
  def set_remote_host_status(hostname: String, port: Int, status: HostStatus) = {
    wrapEx(nameServer.setRemoteHostStatus(hostname, port, status.fromThrift))
  }
  def set_remote_cluster_status(cluster: String, status: HostStatus) = {
    wrapEx(nameServer.setRemoteClusterStatus(cluster, status.fromThrift))
  }

  def get_remote_host(hostname: String, port: Int) = {
    wrapEx(nameServer.getRemoteHost(hostname, port).toThrift)
  }

  def list_remote_clusters(): JList[String] = wrapEx(nameServer.listRemoteClusters)
  def list_remote_hosts(): JList[Host]      = wrapEx(nameServer.listRemoteHosts.map(_.toThrift))

  def list_remote_hosts_in_cluster(cluster: String): JList[Host] = {
    wrapEx(nameServer.listRemoteHosts.map(_.toThrift))
  }
}
