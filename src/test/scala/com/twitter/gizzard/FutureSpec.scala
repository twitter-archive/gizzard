package com.twitter.gizzard

import java.util.concurrent._
import scala.collection.mutable
import com.twitter.util.Time
import com.twitter.conversions.time._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.gizzard.util.Future
import com.twitter.gizzard.thrift.conversions.Sequences._


object FutureSpec extends ConfiguredSpecification with JMocker with ClassMocker {

  "Future" should {
    var future: Future = null

    doBefore {
      future = new Future("test", 1, 1, 1.hour, 50.milliseconds)
    }

    doAfter {
      future.shutdown()
    }

    "execute in the future" in {
      future { 3 * 4 }.get mustEqual 12
    }

    "propagate exceptions that happen in computation" in {
      future { error("whoops"); 1 }.get must throwA[Exception]
    }

    "timeout appropriately" in {
      future { Thread.sleep(2000) }.get(10, TimeUnit.MILLISECONDS) must throwA[TimeoutException]
    }

    "timeout a stuffed-up queue" in {
      Time.withCurrentTimeFrozen { time =>
        future.executor.shutdown()
        future.executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
          def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor) {
            // do nothing.
          }
        })
        future.executor.awaitTermination(1, TimeUnit.MINUTES)
        val f = future { 3 * 4 }
        time.advance(23.seconds)
        f.run()
        f.get(1, TimeUnit.MILLISECONDS) must throwA[Exception]
      }
    }

    "run sequences in parallel" in {
      val runs = future.executor.getCompletedTaskCount
      List(1, 2, 3).parallel(future).map { _ * 2 } mustEqual List(2, 4, 6)
      future.executor.getCompletedTaskCount mustEqual runs + 3
    }

    "run one-element sequences in series" in {
      val runs = future.executor.getCompletedTaskCount
      List(4).parallel(future).map { _ * 2 } mustEqual List(8)
      future.executor.getCompletedTaskCount mustEqual runs
    }
  }

  "ParallelSeq" should {
    var future: Future = null

    doBefore { future = new Future("test", 1, 1, 1.hour, 50.milliseconds) }
    doAfter  { future.shutdown() }

    "map" in {
      Seq(1,2,3).parallel(future).map(_ + 1) must haveTheSameElementsAs(Seq(2,3,4))
    }

    "flatMap" in {
      Seq(1,2,3).parallel(future).flatMap(_ to 4) must haveTheSameElementsAs(Seq(1,2,3,4,2,3,4,3,4))
    }

    "propagate exceptions" in {
      Seq(1,2,3).parallel(future).map(i => if (i == 2) error("whoops") else i) must throwA[Exception]
    }

    "must timeout" in {
      Seq(1,2,3).parallel(future).map { i => Thread.sleep(2000); i } must throwA[TimeoutException]
    }
  }
}
