package com.twitter.gizzard.thrift

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import net.lag.configgy.ConfigMap
import nameserver.NameServer
import scheduler.{CopyJob, CopyJobFactory, JobScheduler, JsonJob, PrioritizingJobScheduler}
import shards.Shard

class GizzardServices[S <: Shard, J <: JsonJob, C <: CopyJob[S]](managerServerPort: Int,
                                                idleTimeout: Duration,
                                                nameServer: NameServer[S],
                                                copyFactory: CopyJobFactory[S],
                                                scheduler: PrioritizingJobScheduler[J],
                                                copyScheduler: JobScheduler[C]) {


  def this(config: gizzard.config.GizzardServices,
           nameServer: NameServer[S],
           copyFactory: CopyJobFactory[S],
           scheduler: PrioritizingJobScheduler[J],
           copyScheduler: JobScheduler[C]) =
    this(config.managerServerPort,
         config.timeout,
         nameServer,
         copyFactory,
         scheduler,
         copyScheduler)

  def this(config: ConfigMap,
           nameServer: NameServer[S],
           copyFactory: CopyJobFactory[S],
           scheduler: PrioritizingJobScheduler[J],
           copyScheduler: JobScheduler[C]) =
    this(config("manager_server_port").toInt,
         config("idle_timeout_sec").toInt.seconds,
         nameServer,
         copyFactory,
         scheduler,
         copyScheduler)

  val gizzardThreadPool = TThreadServer.makeThreadPool("gizzard", 0)

  val managerServer = new ManagerService(nameServer, copyFactory, scheduler, copyScheduler)
  val managerProcessor = new Manager.Processor(managerServer)
  val managerThriftServer = TThreadServer("gizzardManager", managerServerPort, idleTimeout.inMillis.toInt, gizzardThreadPool, managerProcessor, false)

  def start() {
    managerThriftServer.start()
  }

  def shutdown() {
    managerThriftServer.stop()
  }
}
