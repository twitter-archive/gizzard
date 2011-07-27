package com.twitter.gizzard.scheduler

import scala.collection.mutable
import com.twitter.util.Time
import com.twitter.conversions.time._
import net.lag.kestrel.{PersistentQueue, QItem}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.gizzard.ConfiguredSpecification


object MemoryJobQueueSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "MemoryJobQueue" should {
    val job1 = mock[JsonJob]
    val job2 = mock[JsonJob]
    val job3 = mock[JsonJob]
    val destinationQueue = mock[MemoryJobQueue]

    var queue: MemoryJobQueue = null

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
      queue.get() must beSome[Ticket]

      queue.shutdown()
      queue.running mustEqual false
      queue.paused mustEqual true
      queue.get() mustEqual None
    }

    "put & get" in {
      "normal" in {
        queue.put(job1)
        queue.size mustEqual 1
        queue.get() must beSome[Ticket].which { _.job eq job1 }
      }

      "full" in {
        queue.put(job1)
        queue.put(job2)
        (0 until 19).foreach { n => queue.put(job3) }
        queue.size mustEqual 20
        queue.get() must beSome[Ticket].which { _.job eq job2 }
      }
    }

    "drainTo" in {
      expect {
        one(destinationQueue).put(job1)
        one(destinationQueue).put(job2)
      }

      Time.withCurrentTimeFrozen { time =>
        queue.drainTo(destinationQueue, 1.millisecond)
        queue.put(job1)
        queue.put(job2)
        time.advance(2.milliseconds)
        queue.checkExpiration(10)
      }
    }
  }
}
