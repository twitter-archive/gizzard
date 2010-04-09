package com.twitter.gizzard.jobs

import nameserver.{ShardMigration, NameServer}
import shards.Shard
import scheduler.JobScheduler
import scala.reflect.Manifest


class MigrateParser[S <: Shard](copyParser: CopyParser[S])
  extends jobs.UnboundJobParser[(NameServer[S], JobScheduler)] {

  def apply(attributes: Map[String, Any]) = {
    new Migrate(
      copyParser(attributes),
      new ShardMigration(
        attributes("source_shard_id").asInstanceOf[Int],
        attributes("destination_shard_id").asInstanceOf[Int],
        attributes("replicating_shard_id").asInstanceOf[Int],
        attributes("write_only_shard_id").asInstanceOf[Int]))
  }
}

class Migrate[S <: Shard](copy: Copy[S], migration: ShardMigration)
  extends Copy[S](migration.sourceShardId, migration.destinationShardId, copy.count) {

  def serialize = Map(
    "copy_class_name" -> copy.getClass.getName,
    "source_shard_id" -> migration.sourceShardId,
    "destination_shard_id" -> migration.destinationShardId,
    "replicating_shard_id" -> migration.replicatingShardId,
    "write_only_shard_id" -> migration.writeOnlyShardId
  ) ++ copy.serialize

  def copyPage(sourceShard: S, destinationShard: S, count: Int) = {
    copy.copyPage(sourceShard, destinationShard, count)
  }

  override def finish(nameServer: NameServer[S], scheduler: JobScheduler) = {
    copy.finish(nameServer, scheduler)
    ShardMigration.finish(migration, nameServer)
    super.finish(nameServer, scheduler)
  }
}