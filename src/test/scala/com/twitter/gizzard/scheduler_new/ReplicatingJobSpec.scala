package com.twitter.gizzard.scheduler

import com.twitter.rpcclient.Client
import thrift.JobInjectorClient
import thrift.JobInjector
import org.specs.mock.{ClassMocker, JMocker}


class RemoteReplicatingJobSpec extends ConfiguredSpecification with JMocker {
  "RemoteReplicatingJob" should {
    val iface = mock[JobInjector.Iface]
    val client = mock[Client[JobInjector.Iface]]
    val testJsonJobClass = "com.twitter.gizzard.scheduler.TestJsonJob"

    val job1 = mock[JsonJob]

    val job = new RemoteReplicatingJob[JsonJob](client, 1, Seq(job1), false)
    val replicatedJob = new RemoteReplicatingJob[JsonJob](client, 1, Seq(job1), true)

    "toMap" >> {
      expect {
        one(job1).className willReturn testJsonJobClass
        one(job1).toMap willReturn Map[String, Any]()
      }
      val map = job.toMap
      map("replicated") mustEqual false
      val tasks = map("tasks").asInstanceOf[Seq[Map[String, Any]]]
      val taskMap = tasks(0)
      taskMap mustEqual Map(testJsonJobClass -> Map())
    }

    "replicate when replicated is false" in {
      expect {
        one(client).proxy willReturn iface
        one(job1).toJson willReturn """{"some":"1","json":"2"}"""
        one(job1).apply()
        one(iface).inject_jobs(any)
      }

      job.apply()
    }

    "not replicate when replicated is true" in {
      expect {
        one(job1).apply()
      }

      replicatedJob.apply()
    }
  }
}
