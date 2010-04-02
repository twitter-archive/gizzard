package com.twitter.gizzard.jobs

import nameserver.{ShardMigration, NameServer}
import shards.Shard
import scheduler.JobScheduler


class Migrate[S <: Shard](copy: Copy[S], migration: ShardMigration)
  extends Copy[S](migration.sourceShardId, migration.destinationShardId, copy.count) {

  def copyPage(sourceShard: S, destinationShard: S, count: Int) = {
    copy.copyPage(sourceShard, destinationShard, count)
  }

  override def finish(nameServer: NameServer[S], scheduler: JobScheduler) = {
    ShardMigration.finish(migration, nameServer)
    super.finish(nameServer, scheduler)
  }
}