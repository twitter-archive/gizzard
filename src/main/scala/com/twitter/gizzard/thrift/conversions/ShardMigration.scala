package com.twitter.gizzard.thrift.conversions


object ShardMigration {
  class RichShardingShardMigration(shardMigration: sharding.ShardMigration) {
    def toThrift = new thrift.ShardMigration(shardMigration.sourceShardId, shardMigration.destinationShardId,
                                             shardMigration.replicatingShardId, shardMigration.writeOnlyShardId)
    
  }
  implicit def shardingShardMigrationToRichShardingShardMigration(shardMigration: sharding.ShardMigration) = new RichShardingShardMigration(shardMigration)

  class RichThriftShardMigration(shardMigration: thrift.ShardMigration) {
    def fromThrift = new sharding.ShardMigration(shardMigration.source_shard_id, shardMigration.destination_shard_id,
                                        shardMigration.replicating_shard_id, shardMigration.write_only_shard_id)
  }
  implicit def thriftShardMigrationToRichThriftShardMigration(shardMigration: thrift.ShardMigration) = new RichThriftShardMigration(shardMigration)
}
