package com.twitter.gizzard.thrift

import java.util.{List => JList}
import net.lag.logging.Logger
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.thrift.conversions.Busy._
import com.twitter.gizzard.thrift.conversions.ChildInfo._
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import com.twitter.gizzard.thrift.conversions.Forwarding._
import com.twitter.gizzard.thrift.conversions.ShardMigration._
import com.twitter.gizzard.sharding._


class ShardManagerService[S <: Shard](nameServer: NameServer[S]) extends ShardManager.Iface {
  val log = Logger.get(getClass.getName)

  def create_shard(shard: thrift.ShardInfo) = {
    nameServer.createShard(shard.fromThrift)
  }

  def find_shard(shard: thrift.ShardInfo) = {
    nameServer.findShard(shard.fromThrift)
  }

  def get_shard(shardId: Int): thrift.ShardInfo = {
    nameServer.getShard(shardId).toThrift
  }

  def update_shard(shard: thrift.ShardInfo) {
    nameServer.updateShard(shard.fromThrift)
  }

  def delete_shard(shardId: Int) {
    nameServer.deleteShard(shardId)
  }

  def add_child_shard(parentShardId: Int, childShardId: Int, position: Int, weight: Int) {
    nameServer.addChildShard(parentShardId, childShardId, position, weight)
  }

  def remove_child_shard(parentShardId: Int, childShardId: Int) {
    nameServer.removeChildShard(parentShardId, childShardId)
  }

  def replace_child_shard(oldChildShardId: Int, newChildShardId: Int) {
    nameServer.replaceChildShard(oldChildShardId, newChildShardId)
  }

  def list_shard_children(shardId: Int): JList[thrift.ChildInfo] = {
    nameServer.listShardChildren(shardId).map(_.toThrift).toJavaList
  }

  def mark_shard_busy(shardId: Int, busy: Int) {
    nameServer.markShardBusy(shardId, busy.fromThrift)
  }

  def copy_shard(sourceShardId: Int, destinationShardId: Int) {
    nameServer.copyShard(sourceShardId, destinationShardId)
  }

  def setup_migration(sourceShardInfo: thrift.ShardInfo, destinationShardInfo: thrift.ShardInfo) = {
    nameServer.setupMigration(sourceShardInfo.fromThrift, destinationShardInfo.fromThrift).toThrift
  }

  def migrate_shard(migration: thrift.ShardMigration) {
    nameServer.migrateShard(migration.fromThrift)
  }

  def set_forwarding(forwarding: thrift.Forwarding) {
    nameServer.setForwarding(forwarding.fromThrift)
  }

  def replace_forwarding(oldShardId: Int, newShardId: Int) = {
    nameServer.replaceForwarding(oldShardId, newShardId)
  }

  def get_forwarding(tableId: JList[java.lang.Integer], baseId: Long) = {
    nameServer.getForwarding(tableId.toList, baseId).toThrift
  }

  def get_forwarding_for_shard(shardId: Int) = {
    nameServer.getForwardingForShard(shardId).toThrift
  }

  def get_forwardings(): JList[thrift.Forwarding] = {
    nameServer.getForwardings().map(_.toThrift).toJavaList
  }

  def reload_forwardings() {
    log.info("Reloading forwardings...")
    nameServer.reload()
  }

  def find_current_forwarding(tableId: JList[java.lang.Integer], id: Long) = {
    nameServer.findCurrentForwarding(tableId.toList, id).shardInfo.toThrift
  }

  def shard_ids_for_hostname(hostname: String, className: String): JList[java.lang.Integer] = {
    nameServer.shardIdsForHostname(hostname, className).toJavaList
  }

  def shards_for_hostname(hostname: String, className: String): JList[thrift.ShardInfo] = {
    nameServer.shardsForHostname(hostname, className).map(_.toThrift).toJavaList
  }

  def get_busy_shards(): JList[thrift.ShardInfo] = {
    nameServer.getBusyShards().map(_.toThrift).toJavaList
  }

  def get_parent_shard(shardId: Int) = {
    nameServer.getParentShard(shardId).toThrift
  }

  def get_root_shard(shardId: Int) = {
    nameServer.getRootShard(shardId).toThrift
  }

  def get_child_shards_of_class(parentShardId: Int, className: String): JList[thrift.ShardInfo] = {
    nameServer.getChildShardsOfClass(parentShardId, className).map(_.toThrift).toJavaList
  }
}
