package com.twitter.gizzard.sharding

import java.util.{List => JList}
import net.lag.logging.Logger
import com.twitter.gizzard.Conversions._


class ShardManager[Key, S <: Shard](nameServer: NameServer[Key, S]) extends thrift.ShardManager.Iface {
  val log = Logger.get(getClass.getName)

  def create_shard(shard: thrift.ShardInfo) = {
    nameServer.createShard(ShardInfo.fromThrift(shard))
  }

  def find_shard(shard: thrift.ShardInfo) = {
    nameServer.findShard(ShardInfo.fromThrift(shard))
  }

  def get_shard(shardId: Int): thrift.ShardInfo = {
    ShardInfo.toThrift(nameServer.getShard(shardId))
  }

  def update_shard(shard: thrift.ShardInfo) {
    nameServer.updateShard(ShardInfo.fromThrift(shard))
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
    nameServer.listShardChildren(shardId).map { ChildInfo.toThrift(_) }.toJavaList
  }

  def mark_shard_busy(shardId: Int, busy: Int) {
    nameServer.markShardBusy(shardId, Busy(busy))
  }

  def copy_shard(sourceShardId: Int, destinationShardId: Int) {
    // FIXME
  }

  def setup_migration(sourceShardInfo: thrift.ShardInfo, destinationShardInfo: thrift.ShardInfo) = {
    // FIXME
//    new thrift.ShardMigration(...)
    null
  }

  def migrate_shard(migration: thrift.ShardMigration) {
    // FIXME
  }

  def set_forwarding(forwarding: thrift.Forwarding) {
    nameServer.setForwarding(Forwarding.fromThrift(forwarding))
  }

  def replace_forwarding(oldShardId: Int, newShardId: Int) = {
    nameServer.replaceForwarding(oldShardId, newShardId)
  }

  def get_forwarding(tableId: JList[java.lang.Integer], baseId: Long) = {
    ShardInfo.toThrift(nameServer.getForwarding(tableId.toList, baseId))
  }

  def get_forwarding_for_shard(shardId: Int) = {
    Forwarding.toThrift(nameServer.getForwardingForShard(shardId))
  }

  def get_forwardings(): JList[thrift.Forwarding] = {
    nameServer.getForwardings().map { Forwarding.toThrift(_) }.toJavaList
  }

  def reload_forwardings() {
    log.info("Reloading forwardings...")
    nameServer.reload()
  }

  def find_current_forwarding(tableId: JList[java.lang.Integer], id: Long) = {
    ShardInfo.toThrift(nameServer.findCurrentForwarding(tableId.toList, id))
  }

  def shard_ids_for_hostname(hostname: String, className: String): JList[java.lang.Integer] = {
    nameServer.shardIdsForHostname(hostname, className).toJavaList
  }

  def shards_for_hostname(hostname: String, className: String): JList[thrift.ShardInfo] = {
    nameServer.shardsForHostname(hostname, className).map { ShardInfo.toThrift(_) }.toJavaList
  }

  def get_busy_shards(): JList[thrift.ShardInfo] = {
    nameServer.getBusyShards().map { ShardInfo.toThrift(_) }.toJavaList
  }

  def get_parent_shard(shardId: Int) = {
    ShardInfo.toThrift(nameServer.getParentShard(shardId))
  }

  def get_root_shard(shardId: Int) = {
    ShardInfo.toThrift(nameServer.getRootShard(shardId))
  }

  def get_child_shards_of_class(parentShardId: Int, className: String): JList[thrift.ShardInfo] = {
    nameServer.getChildShardsOfClass(parentShardId, className).map { shardInfo: ShardInfo =>
      ShardInfo.toThrift(shardInfo)
    }.toJavaList
  }
}
