package com.twitter.gizzard.thrift.conversions

import conversions.ShardId._


object ShardMigration {
  class RichShardingShardMigration(shardMigration: nameserver.ShardMigration) {
    def toThrift = new thrift.ShardMigration(shardMigration.sourceId.toThrift, shardMigration.destinationId.toThrift)
  }
  implicit def shardingShardMigrationToRichShardingShardMigration(shardMigration: nameserver.ShardMigration) = new RichShardingShardMigration(shardMigration)

  class RichThriftShardMigration(shardMigration: thrift.ShardMigration) {
    def fromThrift = new nameserver.ShardMigration(shardMigration.source_id.fromThrift, shardMigration.destination_id.fromThrift)
  }
  implicit def thriftShardMigrationToRichThriftShardMigration(shardMigration: thrift.ShardMigration) = new RichThriftShardMigration(shardMigration)
}
