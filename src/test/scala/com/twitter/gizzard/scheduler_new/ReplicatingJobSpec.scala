package com.twitter.gizzard.scheduler

import com.twitter.rpcclient.Client
import thrift.JobInjectorClient
import thrift.JobInjector
import org.specs.mock.{ClassMocker, JMocker}


class RemoteReplicatingJobSpec extends ConfiguredSpecification with JMocker {
  "RemoteReplicatingJob" should {
    val injector = mock[Iterable[JsonJob] => Unit]
    val testJsonJobClass = "com.twitter.gizzard.scheduler.TestJsonJob"

    val job1 = mock[JsonJob]

    val job = new RemoteReplicatingJob[JsonJob](injector, Seq(job1))
    val replicatedJob = new RemoteReplicatingJob[JsonJob](injector, Seq(job1), false)

    "toMap" >> {
      expect {
        one(job1).className willReturn testJsonJobClass
        one(job1).toMap willReturn Map[String, Any]()
      }
      val map = job.toMap
      map("should_replicate") mustEqual true
      val tasks = map("tasks").asInstanceOf[Seq[Map[String, Any]]]
      val taskMap = tasks(0)
      taskMap mustEqual Map(testJsonJobClass -> Map())
    }

    "replicate when shouldReplicate is true" in {
      expect {
        //one(job1).toJson willReturn """{"some":"1","json":"2"}"""
        one(job1).apply()
        one(injector).apply(List(job))
      }

      job.apply()
    }

    "not replicate when shouldReplicate is false" in {
      expect {
        one(job1).apply()
      }

      replicatedJob.apply()
    }
  }
}
