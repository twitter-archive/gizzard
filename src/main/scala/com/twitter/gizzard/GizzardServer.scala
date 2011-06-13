package com.twitter.gizzard

import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobScheduler, PrioritizingJobScheduler, ReplicatingJsonCodec}
import com.twitter.gizzard.config.{GizzardServer => ServerConfig}


abstract class GizzardServer(config: ServerConfig) {
  def jobPriorities: Seq[Int]
  def copyPriority: Int

  // setup logging

  Logger.configure(config.loggers)
  protected val log = Logger.get(getClass)

  // nameserver/shard wiring

  lazy val nameServer = config.nameServer()

  // job wiring

  def logUnparsableJob(j: Array[Byte]) {
    log.error("Unparsable job: %s", new String(j) )
  }

  lazy val jobCodec     = new ReplicatingJsonCodec(nameServer.jobRelay, logUnparsableJob)
  lazy val jobScheduler = new PrioritizingJobScheduler(jobPriorities map { p =>
    p -> config.jobQueues(p)(jobCodec)
  } toMap)

  // service wiring

  lazy val managerServer       = new thrift.ManagerService(nameServer, jobScheduler, copyPriority)
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
