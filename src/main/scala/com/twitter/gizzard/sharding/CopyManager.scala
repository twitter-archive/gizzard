package com.twitter.gizzard.sharding

import jobs.{CopyMachine, JobScheduler}


trait CopyManager[S <: Shard] {
  /** Return a scheduler to be used for running copy/migrate jobs. */
  def scheduler: JobScheduler

  /** Create a new CopyMachine job for copying one shard to another. */
  def newCopyJob(sourceShardId: Int, destinationShardId: Int): CopyMachine[S]

  /** Create a new CopyMachine job for migrating one shard to another. */
  def newMigrateJob(migration: ShardMigration): CopyMachine[S]
}
