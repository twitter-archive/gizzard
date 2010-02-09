package com.twitter.gizzard.sharding

import net.lag.logging.Logger
import com.twitter.gizzard.Conversions._


class ShardManager[Key, S <: Shard](nameServer: NameServer[Key, S]) {
  val log = Logger.get(getClass.getName)

  def create_shard(shard: gen.ShardInfo) = {
    nameServer.createShard(shard)
  }

  def find_shard(shard: gen.ShardInfo) = {
    nameServer.findShard(shard)
  }

  def get_shard(shardId: Int): gen.ShardInfo = {
    nameServer.getShard(shardId)
  }

  def update_shard(shard: gen.ShardInfo) {
    nameServer.updateShard(shard)
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

  def list_shard_children(shardId: Int): java.util.List[gen.ShardChild] = {
    nameServer.listShardChildren(shardId).toJavaList
  }

  def mark_shard_busy(shardId: Int, busy: Int) {
    nameServer.markShardBusy(shardId, Busy(busy))
  }

  def set_forwarding(forwarding: gen.Forwarding) {
    nameServer.setForwarding(forwarding)
  }

  def get_forwarding(tableId: java.util.List[java.lang.Integer], baseId: Long) = {
    nameServer.getForwarding(tableId.toSeq, baseId)
  }

  def get_forwardings(): java.util.List[gen.Forwarding] = {
    nameServer.getForwardings().toJavaList
  }

  def get_forwarding_for_shard(shardId: Int) = {
    nameServer.getForwardingForShard(shardId)
  }

  def replace_forwarding(oldShardId: Int, newShardId: Int) = {
    nameServer.replaceForwarding(oldShardId, newShardId)
  }

  def reload_forwardings() {
    log.info("Reloading forwardings...")
    nameServer.reload()
  }

  def find_current_forwarding(tableId: java.util.List[java.lang.Integer], id: Long) = {
    nameServer.findCurrentForwarding(tableId.toSeq, id)
  }

  def shard_ids_for_hostname(hostname: String, className: String): java.util.List[java.lang.Integer] = {
    nameServer.shardIdsForHostname(hostname, className).toJavaList
  }

  def shards_for_hostname(hostname: String, className: String): java.util.List[gen.ShardInfo] = {
    nameServer.shardsForHostname(hostname, className).toJavaList
  }

  def get_busy_shards(): java.util.List[gen.ShardInfo] = {
    nameServer.getBusyShards().toJavaList
  }

  def get_parent_shard(shardId: Int) = {
    nameServer.getParentShard(shardId)
  }

  def get_root_shard(shardId: Int) = {
    nameServer.getRootShard(shardId)
  }

  def get_child_shards_of_class(parentShardId: Int, className: String): java.util.List[gen.ShardInfo] = {
    nameServer.getChildShardsOfClass(parentShardId, className).toJavaList
  }
}
