package com.twitter.gizzard.scheduler

import scala.collection.mutable
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import net.lag.kestrel.{PersistentQueue, QItem}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}

object MemoryJobQueueSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "MemoryJobQueue" should {
    val job1 = mock[Job]
    val job2 = mock[Job]
    val job3 = mock[Job]
    val destinationQueue = mock[MemoryJobQueue[Job]]

    var queue: MemoryJobQueue[Job] = null

    doBefore {
      queue = new MemoryJobQueue("queue", 20)
      queue.start()
    }

    "size" in {
      queue.size mustEqual 0
      queue.put(job1)
      queue.size mustEqual 1
      queue.get()
      queue.size mustEqual 0
    }

    "start, pause, resume, shutdown" in {
      queue.running mustEqual true
      queue.paused mustEqual false
      queue.put(job1)

      queue.pause()
      queue.running mustEqual true
      queue.paused mustEqual true
      queue.get() mustEqual None

      queue.resume()
      queue.running mustEqual true
      queue.paused mustEqual false
      queue.get() must beSome[Ticket[Job]]

      queue.shutdown()
      queue.running mustEqual false
      queue.paused mustEqual true
      queue.get() mustEqual None
    }

    "put & get" in {
      "normal" in {
        queue.put(job1)
        queue.size mustEqual 1
        queue.get() must beSome[Ticket[Job]].which { _.job eq job1 }
      }

      "full" in {
        queue.put(job1)
        queue.put(job2)
        (0 until 19).foreach { n => queue.put(job3) }
        queue.size mustEqual 20
        queue.get() must beSome[Ticket[Job]].which { _.job eq job2 }
      }
    }

    "drainTo" in {
      expect {
        one(destinationQueue).put(job1)
        one(destinationQueue).put(job2)
      }

      queue.drainTo(destinationQueue, 1.millisecond)
      queue.put(job1)
      queue.put(job2)
      Time.advance(2.milliseconds)
      queue.checkExpiration(10)
    }
  }
}
