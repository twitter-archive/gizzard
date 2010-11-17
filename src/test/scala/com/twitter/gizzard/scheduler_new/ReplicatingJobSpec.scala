package com.twitter.gizzard.scheduler

import org.specs.mock.{ClassMocker, JMocker}
import nameserver.{JobRelay, JobRelayCluster}


class RemoteReplicatingJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "RemoteReplicatingJob" should {
    val relay = mock[JobRelay]
    val relayClient = mock[JobRelayCluster]
    val testJsonJobClass = "com.twitter.gizzard.scheduler.TestJsonJob"

    val job1 = mock[JsonJob]

    val job = new RemoteReplicatingJob[JsonJob](relay, Seq(job1), List("c1"))
    val replicatedJob = new RemoteReplicatingJob[JsonJob](relay, Seq(job1), List())

    "toMap" >> {
      expect {
        one(job1).className willReturn testJsonJobClass
        one(job1).toMap willReturn Map[String, Any]()
      }
      val map = job.toMap
      map("dest_clusters") mustEqual List("c1")
      val tasks = map("tasks").asInstanceOf[Seq[Map[String, Any]]]
      val taskMap = tasks(0)
      taskMap mustEqual Map(testJsonJobClass -> Map())
    }

    "replicate when list of clusters is present" in {
      expect {
        one(job1).apply()
        //one(relay).clusters willReturn List("c1")
        one(relayClient).apply(List(job))
        one(relay).apply("c1") willReturn relayClient
      }

      job.apply()
    }

    "not replicate when list of clusters is empty" in {
      expect {
        one(job1).apply()
      }

      replicatedJob.apply()
    }
  }
}
