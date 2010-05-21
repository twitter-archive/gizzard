package com.twitter.gizzard.thrift

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

  def create_shard(shard: ShardInfo) {
    nameServer.createShard(shard.fromThrift)
  }

  def get_shard(id: ShardId): ShardInfo = {
    nameServer.getShard(id.fromThrift).toThrift
  }

  def delete_shard(id: ShardId) {
    nameServer.deleteShard(id.fromThrift)
  }

  def add_link(upId: ShardId, downId: ShardId, weight: Int) {
    nameServer.addLink(upId.fromThrift, downId.fromThrift, weight)
  }

  def remove_link(upId: ShardId, downId: ShardId) {
    nameServer.removeLink(upId.fromThrift, downId.fromThrift)
  }

  def list_upward_links(id: ShardId): java.util.List[LinkInfo] = {
    nameServer.listUpwardLinks(id.fromThrift).map(_.toThrift).toJavaList
  }

  def list_downward_links(id: ShardId): java.util.List[LinkInfo] = {
    nameServer.listDownwardLinks(id.fromThrift).map(_.toThrift).toJavaList
  }

  def mark_shard_busy(id: ShardId, busy: Int) {
    nameServer.markShardBusy(id.fromThrift, busy.fromThrift)
  }

  def copy_shard(sourceId: ShardId, destinationId: ShardId) {
    scheduler(copier(sourceId.fromThrift, destinationId.fromThrift))
  }

/*  def setup_migration(sourceShardInfo: ShardInfo, destinationShardInfo: ShardInfo) = {
    nameserver.ShardMigration.setup(sourceShardInfo.fromThrift, destinationShardInfo.fromThrift, nameServer).toThrift
  }

  def migrate_shard(migration: ShardMigration) {
    scheduler(new Migrate(copier(migration.source_id.fromThrift, migration.destination_id.fromThrift), migration.fromThrift))
  }

  def finish_migration(migration: ShardMigration) {
    nameserver.ShardMigration.finish(migration.fromThrift, nameServer)
  } */

  def set_forwarding(forwarding: Forwarding) {
    nameServer.setForwarding(forwarding.fromThrift)
  }

  def replace_forwarding(oldId: ShardId, newId: ShardId) = {
    nameServer.replaceForwarding(oldId.fromThrift, newId.fromThrift)
  }

  def get_forwarding(tableId: Int, baseId: Long) = {
    nameServer.getForwarding(tableId, baseId).toThrift
  }

  def get_forwarding_for_shard(id: ShardId) = {
    nameServer.getForwardingForShard(id.fromThrift).toThrift
  }

  def get_forwardings(): java.util.List[Forwarding] = {
    nameServer.getForwardings().map(_.toThrift).toJavaList
  }

  def reload_forwardings() {
    log.info("Reloading forwardings...")
    nameServer.reload()
  }

  def find_current_forwarding(tableId: Int, id: Long) = {
    nameServer.findCurrentForwarding(tableId, id).shardInfo.toThrift
  }

  def shards_for_hostname(hostname: String): java.util.List[ShardInfo] = {
    nameServer.shardsForHostname(hostname).map(_.toThrift).toJavaList
  }

  def get_busy_shards(): java.util.List[ShardInfo] = {
    nameServer.getBusyShards().map(_.toThrift).toJavaList
  }

  def get_child_shards_of_class(parentId: ShardId, className: String): java.util.List[ShardInfo] = {
    nameServer.getChildShardsOfClass(parentId.fromThrift, className).map(_.toThrift).toJavaList
  }

  def rebuild_schema() {
    nameServer.rebuildSchema()
  }
}
