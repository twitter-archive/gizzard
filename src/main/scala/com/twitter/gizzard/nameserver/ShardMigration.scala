package com.twitter.gizzard.nameserver

import shards._

object ShardMigration {
  def setup[ConcreteShard <: shards.Shard](sourceShardInfo: ShardInfo, destinationShardInfo: ShardInfo, nameServer: NameServer[ConcreteShard]): ShardMigration = {
    val lastDot = sourceShardInfo.className.lastIndexOf('.')
    val packageName = if (lastDot >= 0) sourceShardInfo.className.substring(0, lastDot + 1) else ""
    val sourceShardId = nameServer.findShard(sourceShardInfo)
    val destinationShardId = nameServer.createShard(destinationShardInfo)

    val writeOnlyShard = new ShardInfo(packageName + "WriteOnlyShard",
      sourceShardInfo.tablePrefix + "_migrate_write_only", "localhost", "", "", Busy.Normal, 0)
    val writeOnlyShardId = nameServer.createShard(writeOnlyShard)
    nameServer.addChildShard(writeOnlyShardId, destinationShardId, 1)

    val replicatingShard = new ShardInfo(packageName + "ReplicatingShard",
      sourceShardInfo.tablePrefix + "_migrate_replicating", "localhost", "", "", Busy.Normal, 0)
    val replicatingShardId = nameServer.createShard(replicatingShard)
    nameServer.replaceChildShard(sourceShardId, replicatingShardId)
    nameServer.addChildShard(replicatingShardId, sourceShardId, 1)
    nameServer.addChildShard(replicatingShardId, writeOnlyShardId, 1)

    nameServer.replaceForwarding(sourceShardId, replicatingShardId)
    new ShardMigration(sourceShardId, destinationShardId, replicatingShardId, writeOnlyShardId)
  }

  def finish[ConcreteShard <: shards.Shard](migration: ShardMigration, nameServer: NameServer[ConcreteShard]) {
    nameServer.removeChildShard(migration.writeOnlyShardId, migration.destinationShardId)
    nameServer.replaceChildShard(migration.replicatingShardId, migration.destinationShardId)
    nameServer.replaceForwarding(migration.replicatingShardId, migration.destinationShardId)
    nameServer.deleteShard(migration.replicatingShardId)
    nameServer.deleteShard(migration.writeOnlyShardId)
    nameServer.deleteShard(migration.sourceShardId)
  }
}

case class ShardMigration(sourceShardId: Int, destinationShardId: Int, replicatingShardId: Int,
                          writeOnlyShardId: Int)
