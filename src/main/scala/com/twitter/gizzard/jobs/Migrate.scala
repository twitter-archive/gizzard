package com.twitter.gizzard.jobs

import nameserver.{ShardMigration}
import shards.{Shard, ShardId}
import com.twitter.xrayspecs.TimeConversions._
import scheduler.JobScheduler


class Migrate[S <: Shard](val copy: Copy[S], migration: ShardMigration)
  extends Copy[S](migration.sourceId, migration.destinationId, copy.count) {

  def this(attributes: Map[String, AnyVal]) = {
    this(
      Class.forName(attributes("copy_class_name").toString).asInstanceOf[Class[Copy[S]]].getConstructor(classOf[Map[String, AnyVal]]).newInstance(attributes),
      new ShardMigration(
        ShardId(attributes("source_shard_hostname").toString, attributes("source_shard_table_prefix").toString),
        ShardId(attributes("destination_shard_hostname").toString, attributes("destination_shard_table_prefix").toString)))
  }

  def serialize = Map(
    "copy_class_name" -> copy.getClass.getName,
    "souce_shard_hostname" -> migration.sourceId.hostname,
    "source_shard_table_prefix" -> migration.sourceId.tablePrefix,
    "destination_shard_hostname" -> migration.destinationId.hostname,
    "destination_shard_table_prefix" -> migration.destinationId.tablePrefix
  ).asInstanceOf[Map[String, AnyVal]] ++ copy.serialize

  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    copy.copyPage(sourceShard, destinationShard, count).map { new Migrate(_, migration) }
  }
}
