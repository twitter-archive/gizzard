package com.twitter.gizzard.scheduler

import scala.collection.mutable
import net.lag.configgy.{Config, ConfigMap}
import net.lag.kestrel.{PersistentQueue, QItem}
import org.specs.Specification
import com.twitter.xrayspecs.Time
import org.specs.mock.{ClassMocker, JMocker}
import jobs.{Job, JobParser}


object KestrelMessageQueueSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "KestrelMessageQueue" should {
    Time.freeze()
    val message1 = "message1"
    val message2 = "message2"
    val queue = mock[PersistentQueue]
    val items = List(message1, message2).map { x => Some(QItem(0, 0, x.getBytes, x.hashCode)) }.toList

    expect { allowing(queue).setup() }
    val kestrelMessageQueue = new KestrelMessageQueue("queue", queue)

    "be empty after shutdown" >> {
      expect {
        one(queue).close()
        one(queue).isClosed willReturn true
      }

      kestrelMessageQueue.shutdown()
      kestrelMessageQueue.map { x => x }.toList mustEqual Nil
    }

    "iteration" >> {
      expect {
        one(queue).isClosed.willReturn(false) then
        one(queue).removeReceive(an[Int], a[Boolean]).willReturn(items(0)) then
        one(queue).confirmRemove(items(0).get.xid) then
        one(queue).isClosed.willReturn(false) then
        one(queue).removeReceive(an[Int], a[Boolean]).willReturn(items(1)) then
        one(queue).confirmRemove(items(1).get.xid) then
        one(queue).isClosed.willReturn(true)
      }
      kestrelMessageQueue.toList mustEqual List(message1, message2)
    }

    "writeTo" in {
      val destinationMessageQueue = mock[MessageQueue[String, String]]

      expect {
        allowing(queue).isClosed willReturn false
        allowing(queue).length willReturn 2
        one(queue).removeReceive(0, true).willReturn(items(0)) then
          one(queue).removeReceive(0, true).willReturn(items(1))
        one(queue).confirmRemove(items(0).get.xid)
        one(queue).confirmRemove(items(1).get.xid)
        one(destinationMessageQueue).put(message1)
        one(destinationMessageQueue).put(message2)
      }

      kestrelMessageQueue.writeTo(destinationMessageQueue)
    }
  }
}
