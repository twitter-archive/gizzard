package com.twitter.gizzard

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import net.lag.configgy.ConfigMap
import nameserver.{NameServer, BasicShardRepository}
import scheduler.{CopyJobFactory, JobScheduler, JsonJob, JobConsumer, PrioritizingJobScheduler, ReplicatingJsonCodec}
import shards.{Shard, ReadWriteShard}


abstract class GizzardServer[S <: Shard, J <: JsonJob](config: gizzard.config.GizzardServer) {
  // job wiring

  def copyFactory: CopyJobFactory[S]
  def badJobQueue: Option[JobConsumer[JsonJob]]
  def jobPriorities: Seq[Int]
  def copyPriority: Int
  def logUnparsableJob(j: Array[Byte]): Unit

  lazy val jobCodec = new ReplicatingJsonCodec(nameServer.jobRelay, logUnparsableJob)

  lazy val jobScheduler = new PrioritizingJobScheduler(Map(jobPriorities.map { p =>
    p -> config.jobQueues(p)(jobCodec, badJobQueue)
  }:_*))

  lazy val copyScheduler = jobScheduler(copyPriority).asInstanceOf[JobScheduler[JsonJob]]


  // nameserver/shard wiring

  def readWriteShardAdapter: ReadWriteShard[S] => S
  def replicationFuture: Option[Future]

  lazy val shardRepo = new BasicShardRepository[S](readWriteShardAdapter, replicationFuture)

  // XXX: move nsQueryEvaluator config into NameServer config trait
  lazy val nameServer = config.nameServer(config.nsQueryEvaluator(), shardRepo, None)


  // service wiring

  lazy val managerServer       = new thrift.ManagerService(nameServer, copyFactory, jobScheduler, copyScheduler)
  lazy val managerThriftServer = config.manager(new thrift.Manager.Processor(managerServer))

  lazy val jobInjectorServer       = new thrift.JobInjectorService(jobCodec, jobScheduler)
  lazy val jobInjectorThriftServer = config.jobInjector(new thrift.JobInjector.Processor(jobInjectorServer))


  def startGizzard() {
    nameServer.reload()
    jobScheduler.start()

    new Thread(new Runnable { def run() { managerThriftServer.serve() } }, "GizzardManagerThread").start()
    new Thread(new Runnable { def run() { jobInjectorThriftServer.serve() } }, "JobInjectorThread").start()
  }

  def shutdownGizzard(quiesce: Boolean) {
    managerThriftServer.stop()
    jobInjectorThriftServer.stop()

    while (quiesce && jobScheduler.size > 0) Thread.sleep(100)
    jobScheduler.shutdown()
  }
}
