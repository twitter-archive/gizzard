package com.twitter.gizzard

import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.gizzard.nameserver.{NameServer, RemoteClusterManager, RollbackLogManager}
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.config.{GizzardServer => ServerConfig}
import com.twitter.gizzard.proxy.LoggingProxy


abstract class GizzardServer(config: ServerConfig) {
  def jobPriorities: Seq[Int]
  def copyPriority: Int

  // setup logging

  protected def configureLogging { Logger.configure(config.loggers) }
  configureLogging

  protected val log = Logger.get(getClass)
  protected val exceptionLog = Logger.get("exception")
  protected def makeLoggingProxy[T <: AnyRef]()(implicit manifest: Manifest[T]): LoggingProxy[T] = config.queryStats[T]()

  // nameserver/shard wiring

  val nameServer           = config.buildNameServer()
  val remoteClusterManager = config.buildRemoteClusterManager()
  val shardManager         = nameServer.shardManager
  val rollbackLogManager   = new RollbackLogManager(nameServer.shard)
  lazy val adminJobManager = new AdminJobManager(nameServer.shardRepository, shardManager, jobScheduler(copyPriority))

  // job wiring

  def logUnparsableJob(j: Array[Byte]) {
    log.error("Unparsable job: %s", new String(j) )
  }

  lazy val jobCodec = new LoggingJsonCodec(
    new ReplicatingJsonCodec(remoteClusterManager.jobRelay, logUnparsableJob),
    config.jobStats,
    logUnparsableJob
  )

  lazy val jobScheduler = new PrioritizingJobScheduler(jobPriorities map { p =>
    p -> config.jobQueues(p)(jobCodec)
  } toMap)

  // service wiring

  lazy val managerServer       = new thrift.ManagerService(nameServer, shardManager, adminJobManager, remoteClusterManager, rollbackLogManager, jobScheduler)
  lazy val managerThriftServer = config.manager(new thrift.Manager.Processor(managerServer))

  lazy val jobInjectorServer       = new thrift.JobInjectorService(jobCodec, jobScheduler)
  lazy val jobInjectorThriftServer = config.jobInjector(new thrift.JobInjector.Processor(jobInjectorServer))

  def startGizzard() {
    nameServer.reload()
    remoteClusterManager.reload()
    jobScheduler.start()

    new Thread(new Runnable { def run() { managerThriftServer.serve() } }, "GizzardManagerThread").start()
    new Thread(new Runnable { def run() { jobInjectorThriftServer.serve() } }, "JobInjectorThread").start()
  }

  def shutdownGizzard(quiesce: Boolean) {
    remoteClusterManager.closeRelay()
    managerThriftServer.stop()
    jobInjectorThriftServer.stop()

    while (quiesce && jobScheduler.size > 0) Thread.sleep(100)
    jobScheduler.shutdown()
  }
}
