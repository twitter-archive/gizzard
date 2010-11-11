package com.twitter.gizzard.thrift

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import net.lag.configgy.ConfigMap
import nameserver.NameServer
import scheduler.{CopyJob, CopyJobFactory, JobScheduler, JsonJob, PrioritizingJobScheduler}
import shards.Shard

class GizzardServices[S <: Shard, J <: JsonJob](shardServerPort: Int,
                                                jobServerPort: Int,
                                                idleTimeout: Duration,
                                                nameServer: NameServer[S],
                                                copyFactory: CopyJobFactory[S],
                                                scheduler: PrioritizingJobScheduler[J],
                                                copyScheduler: JobScheduler[J]) {


  def this(config: gizzard.config.GizzardServices,
           nameServer: NameServer[S],
           copyFactory: CopyJobFactory[S],
           scheduler: PrioritizingJobScheduler[J],
           copyScheduler: JobScheduler[J]) =
    this(config.shardServerPort,
         config.jobServerPort,
         config.timeout,
         nameServer,
         copyFactory,
         scheduler,
         copyScheduler)

  def this(config: ConfigMap,
           nameServer: NameServer[S],
           copyFactory: CopyJobFactory[S],
           scheduler: PrioritizingJobScheduler[J],
           copyScheduler: JobScheduler[J]) =
    this(config("shard_server_port").toInt,
         config("job_server_port").toInt,
         config("idle_timeout_sec").toInt.seconds,
         nameServer,
         copyFactory,
         scheduler,
         copyScheduler)

  val gizzardThreadPool = TThreadServer.makeThreadPool("gizzard", 0)

  val shardServer = new ShardManagerService(nameServer, copyFactory, copyScheduler)
  val shardProcessor = new ShardManager.Processor(shardServer)
  val shardThriftServer = TThreadServer("shards", shardServerPort, idleTimeout.inMillis.toInt, gizzardThreadPool, shardProcessor, false)

  val jobServer = new JobManagerService(scheduler)
  val jobProcessor = new JobManager.Processor(jobServer)
  val jobThriftServer = TThreadServer("jobs", jobServerPort, idleTimeout.inMillis.toInt, gizzardThreadPool, jobProcessor, false)

  def start() {
    shardThriftServer.start()
    jobThriftServer.start()
  }

  def shutdown() {
    shardThriftServer.stop()
    jobThriftServer.stop()
  }
}
