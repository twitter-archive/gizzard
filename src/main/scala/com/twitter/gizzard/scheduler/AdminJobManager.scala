package com.twitter.gizzard.scheduler

import com.twitter.gizzard.shards.ShardId
import com.twitter.gizzard.nameserver.{ShardRepository, ShardManager}

class AdminJobManager(repo: ShardRepository, shardManager: ShardManager, scheduler: JobScheduler) {
  def scheduleCopyJob(from: ShardId, to: ShardId) {
    // XXX: repo shouldn't build the job, but instead provide an appropriate adapter.
    scheduler.put(repo.newCopyJob(shardManager.getShard(from), shardManager.getShard(to)))
  }

  def scheduleRepairJob(ids: Seq[ShardId]) {
    scheduler.put(repo.newRepairJob(ids.map { shardManager.getShard _ }))
  }

  def scheduleDiffJob(ids: Seq[ShardId]) {
    scheduler.put(repo.newDiffJob(ids.map { shardManager.getShard _ }))
  }
}
