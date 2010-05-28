package com.twitter.gizzard.thrift

import scala.reflect.Manifest
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.thrift.conversions.Busy._
import com.twitter.gizzard.thrift.conversions.LinkInfo._
import com.twitter.gizzard.thrift.conversions.ShardId._
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import com.twitter.gizzard.thrift.conversions.Forwarding._
import com.twitter.gizzard.thrift.conversions.ShardMigration._
import com.twitter.gizzard.shards._
import com.twitter.gizzard.jobs.{Migrate, CopyFactory}
import com.twitter.gizzard.nameserver._
import com.twitter.gizzard.scheduler.JobScheduler
import net.lag.logging.Logger

class ShardManagerService[ConcreteShard <: shards.Shard](nameServer: NameServer[ConcreteShard], copier: CopyFactory[ConcreteShard], scheduler: JobScheduler) extends ShardManager.Iface {
  val log = Logger.get(getClass.getName)
  def wrapWithThriftExceptions[A](f: => A): A = {
    try {
      f
    } catch {
      case ex: shards.ShardException =>
        throw new thrift.ShardException(ex.toString)
    }
  }

  def create_shard(shard: ShardInfo) = wrapWithThriftExceptions {
    nameServer.createShard(shard.fromThrift)
  }

  def get_shard(id: ShardId): ShardInfo = wrapWithThriftExceptions {
    nameServer.getShard(id.fromThrift).toThrift
  }

  def delete_shard(id: ShardId) = wrapWithThriftExceptions {
    nameServer.deleteShard(id.fromThrift)
  }

  def add_link(upId: ShardId, downId: ShardId, weight: Int) = wrapWithThriftExceptions {
    nameServer.addLink(upId.fromThrift, downId.fromThrift, weight)
  }

  def remove_link(upId: ShardId, downId: ShardId) = wrapWithThriftExceptions {
    nameServer.removeLink(upId.fromThrift, downId.fromThrift)
  }

  def list_upward_links(id: ShardId): java.util.List[LinkInfo] = wrapWithThriftExceptions {
    nameServer.listUpwardLinks(id.fromThrift).map(_.toThrift).toJavaList
  }

  def list_downward_links(id: ShardId): java.util.List[LinkInfo] = wrapWithThriftExceptions {
    nameServer.listDownwardLinks(id.fromThrift).map(_.toThrift).toJavaList
  }

  def mark_shard_busy(id: ShardId, busy: Int) = wrapWithThriftExceptions {
    nameServer.markShardBusy(id.fromThrift, busy.fromThrift)
  }

  def copy_shard(sourceId: ShardId, destinationId: ShardId) = wrapWithThriftExceptions {
    scheduler(copier(sourceId.fromThrift, destinationId.fromThrift))
  }

  def set_forwarding(forwarding: Forwarding) = wrapWithThriftExceptions {
    nameServer.setForwarding(forwarding.fromThrift)
  }

  def replace_forwarding(oldId: ShardId, newId: ShardId) = wrapWithThriftExceptions {
    nameServer.replaceForwarding(oldId.fromThrift, newId.fromThrift)
  }

  def get_forwarding(tableId: Int, baseId: Long) = wrapWithThriftExceptions {
    nameServer.getForwarding(tableId, baseId).toThrift
  }

  def get_forwarding_for_shard(id: ShardId) = wrapWithThriftExceptions {
    nameServer.getForwardingForShard(id.fromThrift).toThrift
  }

  def get_forwardings(): java.util.List[Forwarding] = wrapWithThriftExceptions {
    nameServer.getForwardings().map(_.toThrift).toJavaList
  }

  def reload_forwardings() = wrapWithThriftExceptions {
    log.info("Reloading forwardings...")
    nameServer.reload()
  }

  def find_current_forwarding(tableId: Int, id: Long) = wrapWithThriftExceptions {
    nameServer.findCurrentForwarding(tableId, id).shardInfo.toThrift
  }

  def shards_for_hostname(hostname: String): java.util.List[ShardInfo] = wrapWithThriftExceptions {
    nameServer.shardsForHostname(hostname).map(_.toThrift).toJavaList
  }

  def get_busy_shards(): java.util.List[ShardInfo] = wrapWithThriftExceptions {
    nameServer.getBusyShards().map(_.toThrift).toJavaList
  }

  def get_child_shards_of_class(parentId: ShardId, className: String): java.util.List[ShardInfo] = wrapWithThriftExceptions {
    nameServer.getChildShardsOfClass(parentId.fromThrift, className).map(_.toThrift).toJavaList
  }

  def rebuild_schema() = wrapWithThriftExceptions {
    nameServer.rebuildSchema()
  }
}
