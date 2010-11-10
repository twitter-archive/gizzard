package com.twitter.gizzard.scheduler

import org.specs.mock.{ClassMocker, JMocker}
import thrift.{JobInjectorService, TThreadServer, JobInjector}


object RemoteReplicatingJobIntegrationSpec extends ConfiguredSpecification with JMocker with ClassMocker{
  "RemoteReplicatingJobIntegration" should {
    // TODO: make configurable
    val port = 12313

    val codec = new JsonCodec[JsonJob]({ badJob =>
      throw new Exception(new String(badJob, "UTF-8"))
    })

    val scheduler = mock[PrioritizingJobScheduler[JsonJob]]
    val service = new JobInjectorService[JsonJob](codec, scheduler)
    val processor = new JobInjector.Processor(service)
    val server = TThreadServer("injector", port, 2, TThreadServer.makeThreadPool("injector", 0), processor)

    val parser = new RemoteReplicatingJobParser[JsonJob](codec, Seq("localhost"), port, 1)

    doBefore {
      server.start()
    }

    doAfter {
      server.stop()
    }

    "replicate and replay jobs" in {
//      val dummyJob = Map("dummy" -> 1, "job" -> true, "blah" -> "blop")
//      parser(dummyJob)
    }
  }
}

