package com.twitter.gizzard.sharding


case class ShardMigration(sourceShardId: Int, destinationShardId: Int, replicatingShardId: Int,
                          writeOnlyShardId: Int)
