package com.twitter.gizzard.shards

trait LinkSource {
  @throws(classOf[shards.ShardException]) def listDownwardLinks(id: ShardId): Seq[LinkInfo]  
  @throws(classOf[shards.ShardException]) def listLinks(): Seq[LinkInfo]
}