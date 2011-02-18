package com.twitter.gizzard.scheduler

import scala.collection.mutable
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import net.lag.kestrel.{PersistentQueue, QItem, OverlaySetting}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object KestrelJobQueueSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "KestrelJobQueue" should {
    val queue = mock[PersistentQueue]
    val queue2 = mock[PersistentQueue]
    val codec = mock[Codec]
    val job1 = mock[JsonJob]
    val job2 = mock[JsonJob]
    val destinationQueue = mock[KestrelJobQueue]

    var kestrelJobQueue: KestrelJobQueue = null

    doBefore {
      kestrelJobQueue = new KestrelJobQueue("queue", queue, codec)
    }

    "size" in {
      expect {
        one(queue).length willReturn 23L
      }

      kestrelJobQueue.size mustEqual 23
    }

    "age" in {
      expect {
        one(queue).currentAge willReturn 23500
      }

      kestrelJobQueue.age mustEqual 23.5
    }

    "start, pause, resume, shutdown" in {
      expect {
        one(queue).maxExpireSweep_=(0)
        one(queue).setup()
      }

      kestrelJobQueue.start()

      expect {
        one(queue).pauseReads()
      }

      kestrelJobQueue.pause()

      expect {
        one(queue).resumeReads()
      }

      kestrelJobQueue.resume()

      expect {
        one(queue).close()
      }

      kestrelJobQueue.shutdown()
    }

    "put" in {
      "success" in {
        expect {
          one(codec).flatten(job1) willReturn "abc".getBytes
          one(queue).add("abc".getBytes) willReturn true
        }

        kestrelJobQueue.put(job1)
      }

      "failure" in {
        expect {
          one(codec).flatten(job1) willReturn "abc".getBytes
          one(queue).add("abc".getBytes) willReturn false
        }

        kestrelJobQueue.put(job1) must throwA[Exception]
      }
    }

    "get" in {
      "after shutdown" in {
        expect {
          one(queue).close()
          one(queue).isClosed willReturn true
        }

        kestrelJobQueue.shutdown()
        kestrelJobQueue.get() mustEqual None
      }

      "item available immediately" in {
        expect {
          allowing(queue).isClosed willReturn false
          one(queue).removeReceive(any[Long], any[Boolean]) willReturn Some(QItem(0, 0, "abc".getBytes, 900))
          one(codec).inflate("abc".getBytes) willReturn job1
          one(queue).confirmRemove(900)
        }

        val ticket = kestrelJobQueue.get()
        ticket must beSome[Ticket].which { _.job == job1 }
        ticket.get.ack()
      }

      "item available eventually" in {
        expect {
          allowing(queue).isClosed willReturn false
          one(queue).removeReceive(any[Long], any[Boolean]).willReturn(None) then
            one(queue).removeReceive(any[Long], any[Boolean]).willReturn(Some(QItem(0, 0, "abc".getBytes, 900)))
          one(codec).inflate("abc".getBytes) willReturn job1
          one(queue).confirmRemove(900)
        }

        val ticket = kestrelJobQueue.get()
        ticket must beSome[Ticket].which { _.job == job1 }
        ticket.get.ack()
      }
    }

    "drainTo" in {
      val expiredQueueOverlay = mock[OverlaySetting[Option[PersistentQueue]]]
      val maxAgeOverlay       = mock[OverlaySetting[Int]]

      expect {
        one(destinationQueue).queue willReturn queue2

        one(queue).expiredQueue willReturn expiredQueueOverlay
        one(expiredQueueOverlay).set(Some(Some(queue2)))

        one(queue).maxAge willReturn maxAgeOverlay
        one(maxAgeOverlay).set(Some(1))
      }

      kestrelJobQueue.drainTo(destinationQueue, 1.millisecond)
    }
  }
}
