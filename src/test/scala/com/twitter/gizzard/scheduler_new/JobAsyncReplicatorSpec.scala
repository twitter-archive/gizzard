package com.twitter.gizzard.scheduler

import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.gizzard.nameserver.{JobRelay, JobRelayCluster, NullJobRelayCluster}
import com.twitter.gizzard.ConfiguredSpecification

class JobAsyncReplicatorSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JobAsyncReplicator" should {
    val relay = mock[JobRelay]
    val job = "testJob".getBytes("ASCII")
    
    def createAsyncReplicator(relay: JobRelay) = {
      ConfiguredSpecification.resetAsyncReplicatorQueues(config)
      config.jobAsyncReplicator(relay)
    }

    "simple queue test" in {
      expect {
        one(relay).apply(anyString).willReturn[JobRelayCluster] { r: JobRelayCluster =>
          one(r).apply(List(job))
        }
        allowing(relay).clusters          willReturn Set("testCluster")
      }

      val replicator = createAsyncReplicator(relay)
      
      replicator.enqueue(job)

      // Allow the dequeue to process this job.
      Thread.sleep(1000)
    }
  }
}
