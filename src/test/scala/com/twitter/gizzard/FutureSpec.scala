package com.twitter.gizzard

import java.util.concurrent._
import scala.collection.mutable
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import thrift.conversions.Sequences._


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

    "timeout appropriately" in {
      future { Thread.sleep(2000) }.get(10, TimeUnit.MILLISECONDS) must throwA[TimeoutException]
    }

    "timeout a stuffed-up queue" in {
      future.executor.shutdown()
      future.executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
        def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor) {
          // do nothing.
        }
      })
      future.executor.awaitTermination(1, TimeUnit.MINUTES)
      Time.freeze
      val f = future { 3 * 4 }
      Time.advance(23.seconds)
      f.run()
      f.get(1, TimeUnit.MILLISECONDS) must throwA[Exception]
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
}
