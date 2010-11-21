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
        one(job1).toJson    willReturn """{"foo":"bar"}"""
        one(job1).className willReturn testJsonJobClass
        one(job1).toMap     willReturn Map[String, Any]()
      }

      val job = new ReplicatingJob[JsonJob](relay, Array(job1), List("c1"))
      val map = job.toMap
      map("dest_clusters") mustEqual List("c1")

      val tasks = map("tasks").asInstanceOf[Seq[Map[String, Any]]]
      val taskMap = tasks(0)
      taskMap mustEqual Map(testJsonJobClass -> Map())
    }

    "replicate when list of clusters is present" in {
      val json = """{"foo":"bar"}"""

      expect {
        one(job1).toJson willReturn json
      }

      val job = new ReplicatingJob[JsonJob](relay, List(job1), List("c1"))

      expect {
        one(job1).apply()
        one(relay).apply("c1").willReturn[JobRelayCluster] { r: JobRelayCluster =>
          one(r).apply(List(json))
        }
      }

      job.apply()
    }

    "not replicate when list of clusters is empty" in {
      expect {
        one(job1).toJson willReturn """{"foo":"bar"}"""
        one(job1).apply()
      }

      val replicatedJob = new ReplicatingJob[JsonJob](relay, Array(job1), Nil)

      replicatedJob.apply()
    }

    "not replicate when serialized is empty" in {
      expect {
        one(job1).apply()
      }

      val replicatedJob = new ReplicatingJob[JsonJob](relay, Array(job1), List("c1"), Nil)

      replicatedJob.apply()
    }
  }
}
