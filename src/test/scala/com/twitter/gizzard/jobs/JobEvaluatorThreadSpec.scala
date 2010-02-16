package com.twitter.gizzard.jobs

import com.twitter.xrayspecs.Eventually
import net.lag.configgy.Config
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object JobEvaluatorThreadSpec extends Specification with JMocker with ClassMocker with Eventually {
  "JobEvaluatorThread" should {
    var errorQueue: MessageQueue = null

    doBefore {
      errorQueue = mock[MessageQueue]
    }

    "pause & resume" in {
      val queue =
        new KestrelMessageQueue("jobs", Config.fromMap(Map("journal" -> "false")),
                                mock[jobs.JobParser], { _ => }, None)
      val executor = new JobEvaluator(queue)
      val executorThread = new JobEvaluatorThread(executor)

      executorThread.start()
      executorThread.running must eventually(beTrue)

      queue.pause()
      executorThread.pauseWork()
      executorThread.running mustBe false

      queue.resume()
      executorThread.resumeWork()
      executorThread.running mustBe true

      queue.pause()
      executorThread.pauseWork()
      executorThread.running mustBe false
    }
  }
}
