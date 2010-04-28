package com.twitter.gizzard.jobs

import nameserver.{ShardMigration, NameServer}
import shards.Shard
import com.twitter.xrayspecs.TimeConversions._
import scheduler.JobScheduler


class Migrate[S <: Shard](val copy: Copy[S], migration: ShardMigration)
  extends Copy[S](migration.sourceShardId, migration.destinationShardId, copy.count) {

  def this(attributes: Map[String, AnyVal]) = {
    this(
      Class.forName(attributes("copy_class_name").toString).asInstanceOf[Class[Copy[S]]].getConstructor(classOf[Map[String, AnyVal]]).newInstance(attributes),
      new ShardMigration(
        attributes("source_shard_id").toInt,
        attributes("destination_shard_id").toInt,
        attributes("replicating_shard_id").toInt,
        attributes("write_only_shard_id").toInt))
  }

  def serialize = Map(
    "copy_class_name" -> copy.getClass.getName,
    "source_shard_id" -> migration.sourceShardId,
    "destination_shard_id" -> migration.destinationShardId,
    "replicating_shard_id" -> migration.replicatingShardId,
    "write_only_shard_id" -> migration.writeOnlyShardId
  ).asInstanceOf[Map[String, AnyVal]] ++ copy.serialize

  def copyPage(sourceShard: S, destinationShard: S, count: Int) = {
    copy.copyPage(sourceShard, destinationShard, count).map { new Migrate(_, migration) }
  }

  override def finish(nameServer: NameServer[S], scheduler: JobScheduler) = {
    ShardMigration.finish(migration, nameServer)
    super.finish(nameServer, scheduler)
  }
}