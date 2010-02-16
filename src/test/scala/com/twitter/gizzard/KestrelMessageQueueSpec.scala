package com.twitter.gizzard

import scala.collection.mutable
import net.lag.configgy.{Config, ConfigMap}
import net.lag.kestrel.{PersistentQueue, QItem}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import jobs.{Job, JobParser}


object KestrelMessageQueueSpec extends Specification with JMocker with ClassMocker {
  "KestrelMessageQueue" should {
    var kestrelMessageQueue: KestrelMessageQueue = null
    var jobParser: JobParser = null
    var job1: Job = null
    var job2: Job = null
    var queue: PersistentQueue = null
    val items = List("job1", "job2").map { x: String => Some(QItem(0, 0, x.getBytes, 0)) }.toList
    var errors = new mutable.ListBuffer[String]

    doBefore {
      errors.clear()
      jobParser = mock[JobParser]
      job1 = mock[Job]
      job2 = mock[Job]
      queue = mock[PersistentQueue]
      kestrelMessageQueue =
        new KestrelMessageQueue("queue", Config.fromMap(Map("journal" -> "false")), jobParser,
                                { errors += _ }, None)
      kestrelMessageQueue.queue = queue
    }

    "be empty after shutdown" in {
      expect {
        one(queue).close()
        one(queue).isClosed willReturn true
      }

      kestrelMessageQueue.shutdown()
      kestrelMessageQueue.get() mustEqual None
    }

    "processes each item in the queue" in {
      expect {
        one(jobParser).apply("job1") willReturn job1
        one(jobParser).apply("job2") willReturn job2
        allowing(queue).isClosed willReturn false
        one(queue).removeReceive(0, true).willReturn(items(0)) then
          one(queue).removeReceive(0, true).willReturn(items(1)) then
          one(queue).removeReceive(0, true).willReturn(None)
        exactly(2).of(queue).confirmRemove(0)
      }
      kestrelMessageQueue.map { x => x }.toList mustEqual List(job1, job2)
    }

    "log when a job errors" in {
      expect {
        one(jobParser).apply("job1") willThrow(new Exception("zoinks!"))
        one(jobParser).apply("job2") willReturn job2
        allowing(queue).isClosed willReturn false
        one(queue).removeReceive(0, true).willReturn(items(0)) then
          one(queue).removeReceive(0, true).willReturn(items(1)) then
          one(queue).removeReceive(0, true).willReturn(None)
        exactly(2).of(queue).confirmRemove(0)
      }
      kestrelMessageQueue.map { x => x }.toList mustEqual List(job2)
      errors.toList mustEqual List("job1")
    }

    "writeTo" in {
      val destinationMessageQueue = mock[MessageQueue]

      expect {
        allowing(queue).isClosed willReturn false
        one(queue).length willReturn 2
        one(queue).removeReceive(0, true).willReturn(items(0)) then
          one(queue).removeReceive(0, true).willReturn(items(1))
        exactly(2).of(queue).confirmRemove(0)
        one(jobParser).apply("job1") willReturn job1
        one(jobParser).apply("job2") willReturn job2
        one(destinationMessageQueue).put(job1)
        one(destinationMessageQueue).put(job2)
      }

      kestrelMessageQueue.writeTo(destinationMessageQueue)
    }
  }
}
