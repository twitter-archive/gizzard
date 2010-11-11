package com.twitter.gizzard.scheduler

import java.util.concurrent.atomic.AtomicInteger
import org.specs.mock.{ClassMocker, JMocker}
import net.lag.configgy.{Config => CConfig}
import com.twitter.util.TimeConversions._
import thrift.{JobInjectorService, TThreadServer, JobInjector}
import java.io.File


object RemoteReplicatingJobIntegrationSpec extends ConfiguredSpecification with JMocker with ClassMocker{
  "RemoteReplicatingJobIntegration" should {
    // TODO: make configurable
    val port     = 12313
    val injector = new ReplicatingJobInjector(List("localhost"), port, 1, false, 1.second)

    val codec = new ReplicatingJsonCodec(injector, { badJob =>
      println(new String(badJob, "UTF-8"))
    })

    var jobsApplied = new AtomicInteger

    val testJobParser = new JsonJobParser[JsonJob] {
      def apply(json: Map[String, Any]) = new JsonJob {
        override def className = "TestJob"
        def apply() { println("applied"); jobsApplied.incrementAndGet }
        def toMap = json
      }
    }
    codec += ("TestJob".r -> testJobParser)

    val scheduler = PrioritizingJobScheduler(CConfig.fromString("""
      path = "/tmp"
      test {
        threads         = 3
        replay_interval = 3600
        error_limit     = 10
        job_queue       = "tbird_test_q"
        error_queue     = "tbird_test_q_errors"
      }
    """), codec, Map(1 -> "test"), None)

    val service   = new JobInjectorService[JsonJob](codec, scheduler)
    val processor = new JobInjector.Processor(service)
    val server    = TThreadServer("injector", port, 500, TThreadServer.makeThreadPool("injector", 5), processor)

    doBefore {
      server.start()
      scheduler.start()
    }

    doAfter {
      //server.stop()
      scheduler.shutdown()
      new File("/tmp/tbird_test_q").delete()
      new File("/tmp/tbird_test_q_errors").delete()
    }

    "replicate and replay jobs" in {
      val testJob = testJobParser(Map("dummy" -> 1, "job" -> true, "blah" -> "blop"))
      scheduler.put(1, testJob)

      Thread.sleep(500)

      println(jobsApplied.get)
    }
  }
}

