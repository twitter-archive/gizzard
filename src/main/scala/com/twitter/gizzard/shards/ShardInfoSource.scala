package com.twitter.gizzard.shards

trait ShardInfoSource {
  @throws(classOf[shards.ShardException]) def getShard(id: ShardId): ShardInfo
  @throws(classOf[shards.ShardException]) def listShards: Seq[ShardInfo]
}