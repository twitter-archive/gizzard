package com.twitter.gizzard.thrift

import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import nameserver.NameServer
import scheduler.{CopyJob, CopyJobFactory, Job, JobScheduler, JsonJob, PrioritizingJobScheduler}
import shards.Shard

class GizzardServices[S <: Shard, J <: JsonJob](config: ConfigMap,
                                                nameServer: NameServer[S],
                                                copyFactory: CopyJobFactory[S],
                                                scheduler: PrioritizingJobScheduler[J],
                                                copyScheduler: JobScheduler[J]) {

  val shardServerPort = config("shard_server_port").toInt
  val jobServerPort = config("job_server_port").toInt

  val idleTimeout = config("idle_timeout_sec").toInt * 1000
  val gizzardThreadPool = TThreadServer.makeThreadPool("gizzard", 0)

  val shardServer = new ShardManagerService(nameServer, copyFactory, copyScheduler)
  val shardProcessor = new ShardManager.Processor(shardServer)
  val shardThriftServer = TThreadServer("shards", shardServerPort, idleTimeout, gizzardThreadPool, shardProcessor)

  val jobServer = new JobManagerService(scheduler)
  val jobProcessor = new JobManager.Processor(jobServer)
  val jobThriftServer = TThreadServer("jobs", jobServerPort, idleTimeout, gizzardThreadPool, jobProcessor)

  def start() {
    shardThriftServer.start()
    jobThriftServer.start()
  }

  def shutdown() {
    shardThriftServer.stop()
    jobThriftServer.stop()
  }
}
