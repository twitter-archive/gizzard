package com.twitter.gizzard.sharding


case class ShardMigration(sourceShardId: Int, destinationShardId: Int, replicatingShardId: Int,
                          writeOnlyShardId: Int)

object ShardMigration {
  def toThrift(migration: ShardMigration) = {
    new thrift.ShardMigration(migration.sourceShardId, migration.destinationShardId,
                              migration.replicatingShardId, migration.writeOnlyShardId)
  }

  def fromThrift(migration: thrift.ShardMigration) = {
    new ShardMigration(migration.source_shard_id, migration.destination_shard_id,
                       migration.replicating_shard_id, migration.write_only_shard_id)
  }
}
