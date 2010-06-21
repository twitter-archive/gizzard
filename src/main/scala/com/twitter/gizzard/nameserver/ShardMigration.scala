package com.twitter.gizzard.nameserver

import shards._

object ShardMigration {
/*  def setup[ConcreteShard <: shards.Shard](sourceShardInfo: ShardInfo, destinationShardInfo: ShardInfo, nameServer: NameServer[ConcreteShard]): ShardMigration = {
    if (sourceShardInfo.id == destinationShardInfo.id) throw new ShardException("Cannot migrate to the same shard")

    val lastDot = sourceShardInfo.className.lastIndexOf('.')
    val sourceId = sourceShardInfo.id
    nameServer.createShard(destinationShardInfo)
    val destinationId = destinationShardInfo.id

    val writeOnlyShard = new ShardInfo(ShardId("localhost", sourceShardInfo.tablePrefix + "_migrate_write_only"),
      "com.twitter.gizzard.shards.WriteOnlyShard", "", "", Busy.Normal)
    nameServer.createShard(writeOnlyShard)
    val writeOnlyId = writeOnlyShard.id
    nameServer.addLink(writeOnlyId, destinationId, 1)

    val replicatingShard = new ShardInfo(ShardId("localhost", sourceShardInfo.tablePrefix + "_migrate_replicating"),
      "com.twitter.gizzard.shards.ReplicatingShard", "", "", Busy.Normal)
    nameServer.createShard(replicatingShard)
    val replicatingId = replicatingShard.id
    nameServer.listUpwardLinks(sourceId).foreach { link =>
      nameServer.addLink(link.upId, replicatingId, link.weight)
      nameServer.removeLink(link.upId, link.downId)
    }

    nameServer.addLink(replicatingId, sourceId, 1)
    nameServer.addLink(replicatingId, writeOnlyId, 1)

    nameServer.replaceForwarding(sourceId, replicatingId)
    new ShardMigration(sourceId, destinationId, replicatingId, writeOnlyId)
  }

  def finish[ConcreteShard <: shards.Shard](migration: ShardMigration, nameServer: NameServer[ConcreteShard]) {
    nameServer.removeLink(migration.writeOnlyId, migration.destinationId)
    nameServer.listUpwardLinks(migration.replicatingId).foreach { link =>
      nameServer.addLink(link.upId, migration.destinationId, link.weight)
      nameServer.removeLink(link.upId, link.downId)
    }

    nameServer.replaceForwarding(migration.replicatingId, migration.destinationId)
    nameServer.deleteShard(migration.replicatingId)
    nameServer.deleteShard(migration.writeOnlyId)
    nameServer.deleteShard(migration.sourceId)
  } */
} 

case class ShardMigration(sourceId: ShardId, destinationId: ShardId)
