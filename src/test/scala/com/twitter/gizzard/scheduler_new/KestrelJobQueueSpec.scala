package com.twitter.gizzard.scheduler_new

import scala.collection.mutable
import com.twitter.xrayspecs.Time
import net.lag.kestrel.{PersistentQueue, QItem}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object KestrelJobQueueSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "KestrelJobQueue" should {
    val queue = mock[PersistentQueue]
    val codec = mock[Codec[Job[String]]]
    val job1 = mock[Job[String]]
    val job2 = mock[Job[String]]
    val destinationQueue = mock[JobQueue[Job[String]]]

    var kestrelJobQueue: KestrelJobQueue[Job[String]] = null

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
        ticket must beSome[Ticket[Job[String]]].which { _.job == job1 }
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
        ticket must beSome[Ticket[Job[String]]].which { _.job == job1 }
        ticket.get.ack()
      }
    }

    "drainTo" in {
      "normal" in {
        val message1 = "message1".getBytes
        val message2 = "message2".getBytes
        val items = List(message1, message2).map { x => Some(QItem(0, 0, x, x.hashCode)) }.toList

        expect {
          allowing(queue).isClosed willReturn false
          one(queue).length willReturn 2
          one(queue).removeReceive(0, true).willReturn(items(0)) then
            one(queue).removeReceive(0, true).willReturn(items(1))
          one(queue).confirmRemove(items(0).get.xid)
          one(queue).confirmRemove(items(1).get.xid)
          one(codec).inflate(items(0).get.data) willReturn job1
          one(codec).inflate(items(1).get.data) willReturn job2
          one(destinationQueue).put(job1)
          one(destinationQueue).put(job2)
        }

        kestrelJobQueue.drainTo(destinationQueue)
      }

      "after shutdown" in {
        expect {
          one(queue).length willReturn 12
          one(queue).isClosed willReturn true
        }

        kestrelJobQueue.drainTo(destinationQueue)
      }
    }
  }
}
