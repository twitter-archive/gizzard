package com.twitter.gizzard

import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.gizzard.nameserver.{NameServer, RemoteClusterManager}
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.config.{GizzardServer => ServerConfig}
import com.twitter.gizzard.proxy.LoggingProxy
import com.twitter.gizzard.thrift.{ManagerService, JobInjectorService}


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

  lazy val managerService = new ManagerService(
    config.manager.name,
    config.manager.port,
    nameServer,
    shardManager,
    adminJobManager,
    remoteClusterManager,
    jobScheduler
  )

  lazy val jobInjectorService = new JobInjectorService(
    config.jobInjector.name,
    config.jobInjector.port,
    jobCodec,
    jobScheduler
  )

  def startGizzard() {
    nameServer.reload()
    remoteClusterManager.reload()
    jobScheduler.start()
    managerService.start()
    jobInjectorService.start()
  }

  def shutdownGizzard(quiesce: Boolean) {
    remoteClusterManager.closeRelay()
    managerService.shutdown()
    jobInjectorService.shutdown()

    while (quiesce && jobScheduler.size > 0) Thread.sleep(100)
    jobScheduler.shutdown()
  }
}
