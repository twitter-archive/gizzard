package com.twitter.gizzard.thrift

import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import jobs.CopyFactory
import nameserver.NameServer
import scheduler.PrioritizingJobScheduler
import shards.Shard


class GizzardServices[S <: Shard](config: ConfigMap, nameServer: NameServer,
                                  copyFactory: CopyFactory[S],
                                  scheduler: PrioritizingJobScheduler, copyPriority: Int) {

  val shardServerPort = config("shard_server_port").toInt
  val jobServerPort = config("job_server_port").toInt

  val shardServer = new ShardManagerService(nameServer, copyFactory, scheduler(copyPriority))
  val shardProcessor = new ShardManager.Processor(shardServer)
  val shardThriftServer = TSelectorServer("shards", shardServerPort, config, shardProcessor)

  val jobServer = new JobManagerService(scheduler)
  val jobProcessor = new JobManager.Processor(jobServer)
  val jobThriftServer = TSelectorServer("jobs", jobServerPort, config, jobProcessor)

  def start() {
    shardThriftServer.serve()
    jobThriftServer.serve()
  }

  def shutdown() {
    shardThriftServer.shutdown()
    jobThriftServer.shutdown()
  }
}
