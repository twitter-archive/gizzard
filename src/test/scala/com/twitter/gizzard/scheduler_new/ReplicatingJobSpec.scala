package com.twitter.gizzard.scheduler

import com.twitter.rpcclient.Client
import thrift.JobInjectorClient
import thrift.JobInjector
import org.specs.mock.{ClassMocker, JMocker}


class RemoteReplicatingJobSpec extends ConfiguredSpecification with JMocker {
  "RemoteReplicatingJob" should {
    val injector = Map("c1" -> mock[Iterable[JsonJob] => Unit])
    val testJsonJobClass = "com.twitter.gizzard.scheduler.TestJsonJob"

    val job1 = mock[JsonJob]

    val job = new RemoteReplicatingJob[JsonJob](injector, Seq(job1))
    val replicatedJob = new RemoteReplicatingJob[JsonJob](injector, Seq(job1), List())

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
        //one(job1).toJson willReturn """{"some":"1","json":"2"}"""
        one(job1).apply()
        one(injector("c1")).apply(List(job))
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
