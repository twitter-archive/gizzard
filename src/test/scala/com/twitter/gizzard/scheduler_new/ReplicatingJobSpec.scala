package com.twitter.gizzard.scheduler

import org.specs.mock.{ClassMocker, JMocker}
import nameserver.{JobRelay, JobRelayCluster, Host}


class ReplicatingJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "ReplicatingJob" should {
    val relay = mock[JobRelay]
    val testJsonJobClass = "com.twitter.gizzard.scheduler.TestJsonJob"

    val job1 = mock[JsonJob]

    "toMap" in {
      expect {
        atLeast(1).of(job1).className willReturn testJsonJobClass
        atLeast(1).of(job1).toMap     willReturn Map[String, Any]()
      }

      val job = new ReplicatingJob[JsonJob](relay, Seq(job1), List("c1"))
      val map = job.toMap
      map("dest_clusters") mustEqual List("c1")

      val tasks = map("tasks").asInstanceOf[Seq[Map[String, Any]]]
      val taskMap = tasks(0)
      taskMap mustEqual Map(testJsonJobClass -> Map())
    }

    "replicate when list of clusters is present" in {
      expect {
        atLeast(1).of(job1).className willReturn testJsonJobClass
        atLeast(1).of(job1).toMap     willReturn Map[String, Any]()
      }

      val job     = new ReplicatingJob[JsonJob](relay, Seq(job1), List("c1"))
      val payload = new ReplicatingJob[JsonJob](relay, Seq(job1), Nil).toJson

      expect {
        one(job1).apply()
        one(relay).apply("c1").willReturn[JobRelayCluster] { r: JobRelayCluster =>
          one(r).apply(List(payload))
        }
      }

      job.apply()
    }

    "not replicate when list of clusters is empty" in {
      expect {
        one(job1).apply()
      }

      val replicatedJob = new ReplicatingJob[JsonJob](relay, Seq(job1), Nil)

      replicatedJob.apply()
    }
  }
}
