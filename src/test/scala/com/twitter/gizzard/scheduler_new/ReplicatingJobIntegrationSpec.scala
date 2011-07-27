package com.twitter.gizzard.scheduler

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.conversions.time._

import com.twitter.gizzard
import com.twitter.gizzard.thrift.{JobInjectorService, TThreadServer, JobInjector}
import com.twitter.gizzard.nameserver.{Host, HostStatus, JobRelay}
import com.twitter.gizzard.ConfiguredSpecification


object ReplicatingJobIntegrationSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "ReplicatingJobIntegration" should {
    // TODO: make configurable
    val port  = 12313
    val host  = Host("localhost", port, "c1", HostStatus.Normal)
    val relay = new JobRelay(Map("c1" -> List(host)), 1, 1.second, 0)
    val codec = new ReplicatingJsonCodec(relay, { badJob =>
      println(new String(badJob, "UTF-8"))
    })

    var jobsApplied = new AtomicInteger

    val testJobParser = new JsonJobParser {
      def apply(json: Map[String, Any]) = new JsonJob {
        override def className = "TestJob"
        def apply() { jobsApplied.incrementAndGet }
        def toMap = json
      }
    }
    codec += "TestJob".r -> testJobParser

    val schedulerConfig = new gizzard.config.Scheduler {
      val name = "tbird_test_q"
      val schedulerType = new gizzard.config.KestrelScheduler {
        path = "/tmp"
        keepJournal = false
      }

      errorLimit = 10
    }

    val scheduler = new PrioritizingJobScheduler(Map(
      1 -> schedulerConfig(codec)
    ))

    val queue = scheduler(1).queue.asInstanceOf[KestrelJobQueue].queue

    val service   = new JobInjectorService(codec, scheduler)
    val processor = new JobInjector.Processor(service)
    val server    = TThreadServer("injector", port, 500, TThreadServer.makeThreadPool("injector", 5), processor)

    doBefore {
      server.start()
      scheduler.start()
      queue.flush()
    }

    doAfter {
      server.stop()
      scheduler.shutdown()
    }

    "replicate and replay jobs" in {
      val testJob = testJobParser(Map("dummy" -> 1, "job" -> true, "blah" -> "blop"))
      scheduler.put(1, testJob)

      jobsApplied.get must eventually(be_==(2))
    }

    "replicate and replay nested jobs" in {
      val jobs = Seq(
        Map("asdf" -> 1, "bleep" -> "bloop"),
        Map("this" -> 4, "is" -> true, "a test job" -> "teh"))
      val nestedJsonJob = new JsonNestedJob(jobs.map(testJobParser(_)))

      scheduler.put(1, nestedJsonJob)
      jobsApplied.get must eventually(be_==(4))
    }
  }
}

