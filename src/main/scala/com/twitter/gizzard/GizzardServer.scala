package com.twitter.gizzard

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import com.twitter.logging.Logger
import nameserver.{NameServer, BasicShardRepository}
import scheduler.{CopyJobFactory, JobScheduler, JsonJob, JobConsumer, PrioritizingJobScheduler, ReplicatingJsonCodec, RepairJobFactory}
import shards.{Shard, ReadWriteShard}
import config.{GizzardServer => ServerConfig}


abstract class GizzardServer[S <: Shard](config: ServerConfig) {

  def readWriteShardAdapter: ReadWriteShard[S] => S
  def copyFactory: CopyJobFactory[S]
  def repairFactory: RepairJobFactory[S] = null
  def diffFactory: RepairJobFactory[S] = null
  def jobPriorities: Seq[Int]
  def copyPriority: Int
  def repairPriority: Int = copyPriority
  def start(): Unit
  def shutdown(quiesce: Boolean): Unit
  def shutdown() { shutdown(false) }

  // setup logging

  Logger.configure(config.loggers)
  protected val log = Logger.get(getClass)

  // nameserver/shard wiring

  val replicationFuture: Option[Future] = None
  lazy val shardRepo    = new BasicShardRepository[S](readWriteShardAdapter, replicationFuture)
  lazy val nameServer   = config.nameServer(shardRepo)


  // job wiring

  def logUnparsableJob(j: Array[Byte]) {
    log.error("Unparsable job: %s", new String(j) )
  }

  lazy val jobCodec     = new ReplicatingJsonCodec(nameServer.jobRelay, logUnparsableJob)
  lazy val jobScheduler = new PrioritizingJobScheduler(jobPriorities map { p =>
    p -> config.jobQueues(p)(jobCodec)
  } toMap)

  lazy val copyScheduler = jobScheduler(copyPriority).asInstanceOf[JobScheduler]


  // service wiring

  lazy val managerServer = new thrift.ManagerService(
    nameServer,
    copyFactory,
    jobScheduler,
    copyScheduler,
    repairFactory,
    repairPriority,
    diffFactory)

  lazy val managerThriftServer = config.manager(new thrift.Manager.Processor(managerServer))

  lazy val jobInjectorServer       = new thrift.JobInjectorService(jobCodec, jobScheduler)
  lazy val jobInjectorThriftServer = config.jobInjector(new thrift.JobInjector.Processor(jobInjectorServer))

  def loggingProxy[T <: AnyRef](obj: T)(implicit manifest: Manifest[T]) = config.stats(obj)

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
