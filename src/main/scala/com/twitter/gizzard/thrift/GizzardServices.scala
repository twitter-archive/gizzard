package com.twitter.gizzard.thrift

import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import nameserver.NameServer
import scheduler.{CopyJob, CopyJobFactory, Job, PrioritizingJobScheduler}
import shards.Shard


class GizzardServices[S <: Shard, J <: Job[_], C <: CopyJob[S, C]](config: ConfigMap,
                                  nameServer: NameServer[S],
                                  copyFactory: CopyJobFactory[S, C],
                                  scheduler: PrioritizingJobScheduler[J], copyPriority: Int) {

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
