package com.twitter.gizzard.nameserver


case class ShardMigration(sourceShardId: Int, destinationShardId: Int, replicatingShardId: Int,
                          writeOnlyShardId: Int)
