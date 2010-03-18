package com.twitter.gizzard.scheduler

import scala.collection.mutable
import net.lag.configgy.{Config, ConfigMap}
import net.lag.kestrel.{PersistentQueue, QItem}
import org.specs.Specification
import com.twitter.ostrich.DevNullStats
import com.twitter.xrayspecs.Time
import org.specs.mock.{ClassMocker, JMocker}
import jobs.{Job, JobParser}


object KestrelMessageQueueSpec extends Specification with JMocker with ClassMocker {
  "KestrelMessageQueue" should {
    Time.freeze()
    val message1 = "message1"
    val message2 = "message2"
    val queue = mock[PersistentQueue]
    val items = List(message1, message2).map { x: String => Some(QItem(0, 0, x.getBytes, 0)) }.toList

    expect { allowing(queue).setup() }
    val kestrelMessageQueue = new KestrelMessageQueue("queue", queue, DevNullStats)

    "be empty after shutdown" >> {
      expect {
        one(queue).close()
        one(queue).isClosed willReturn true
      }

      kestrelMessageQueue.shutdown()
      kestrelMessageQueue.map { x => x }.toList mustEqual Nil
    }

    "iteration" >> {
      val till = Time.now.inMillis + kestrelMessageQueue.TIMEOUT
      expect {
        one(queue).isClosed.willReturn(false) then
        one(queue).removeReceive(till, true).willReturn(items(0)) then
        one(queue).confirmRemove(0) then
        one(queue).isClosed.willReturn(false) then
        one(queue).removeReceive(till, true).willReturn(items(1)) then
        one(queue).confirmRemove(0) then
        one(queue).isClosed.willReturn(true)
      }
      kestrelMessageQueue.toList mustEqual List(message1, message2)
    }

    "writeTo" in {
      val destinationMessageQueue = mock[MessageQueue[String, String]]

      expect {
        allowing(queue).isClosed willReturn false
        one(queue).length willReturn 2
        one(queue).removeReceive(0, true).willReturn(items(0)) then
          one(queue).removeReceive(0, true).willReturn(items(1))
        exactly(2).of(queue).confirmRemove(0)
        one(destinationMessageQueue).put(message1)
        one(destinationMessageQueue).put(message2)
      }

      kestrelMessageQueue.writeTo(destinationMessageQueue)
    }
  }
}
